import { readCar as iterateCar } from "@atcute/car";
import { decode, decodeFirst, fromBytes, toCidLink } from "@atcute/cbor";
import type { ComAtprotoSyncSubscribeRepos } from "@atcute/atproto";
import { CID } from "multiformats/cid";
import { ThreadWorker } from "@poolifier/poolifier-web-worker";
import { BackgroundQueue, Database } from "@atproto/bsky";
import { IndexingService } from "@atproto/bsky/dist/data-plane/server/indexing/index.js";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { BlobRef } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import type { FirehoseSubscriptionOptions } from "./subscription.ts";

declare const workerData: WorkerData;

if (!workerData) {
	throw new Error("must be run as a worker");
}

type WorkerData = Pick<
	FirehoseSubscriptionOptions,
	"dbOptions" | "idResolverOptions"
>;

export type WorkerInput = {
	chunk: Uint8Array;
};

export type WorkerOutput = {
	success?: boolean;
	cursor?: number;
	error?: unknown;
};

const { dbOptions, idResolverOptions } = workerData;
if (!dbOptions || !idResolverOptions) {
	throw new Error("worker missing options");
}

class Worker extends ThreadWorker<WorkerInput, WorkerOutput> {
	db = new Database(dbOptions);
	idResolver = new IdResolver({
		...idResolverOptions,
		didCache: new MemoryCache(),
	});
	background = new BackgroundQueue(this.db);
	indexingSvc = new IndexingService(this.db, this.idResolver, this.background);

	constructor() {
		// must be async; poolifier uses that to determine whether to await
		super(async (data) => this.process(data!), { maxInactiveTime: 120_000 });
	}

	process = async ({ chunk }: WorkerInput): Promise<WorkerOutput> => {
		try {
			const event = decodeChunk(chunk);
			if (!event) return { success: true };
			const { success, cursor, error } = await this.tryIndexEvent(event);
			if (success) {
				return { success, cursor };
			} else {
				return { success, error };
			}
		} catch (err) {
			return { success: false, error: err };
		}
	};

	async tryIndexEvent(
		event: Event,
	): Promise<{ success: boolean; cursor?: number; error?: unknown }> {
		let attempt = 0;

		let err: unknown;
		while (attempt < 5) {
			try {
				// todo: some sort of did mutex across workers
				await this.indexEvent(event);
				return { success: true, cursor: event.seq };
			} catch (e) {
				attempt++;
				err = e;
			}
		}

		return {
			success: false,
			error: `max attempts reached for ${event.did} ${event.seq}\n${err}`,
		};
	}

	async indexEvent(event: Event) {
		if (!event) return { success: true };

		try {
			if (event.$type === "com.atproto.sync.subscribeRepos#identity") {
				await this.indexingSvc.indexHandle(event.did, event.time, true);
			} else if (event.$type === "com.atproto.sync.subscribeRepos#account") {
				if (event.active === false && event.status === "deleted") {
					await this.indexingSvc.deleteActor(event.did);
				} else {
					await this.indexingSvc.updateActorStatus(
						event.did,
						event.active,
						event.status,
					);
				}
			} else if (event.$type === "com.atproto.sync.subscribeRepos#sync") {
				const cid = parseCid(iterateCar(event.blocks).header.data.roots[0]);
				await Promise.all([
					this.indexingSvc.setCommitLastSeen(event.did, cid, event.rev),
					this.indexingSvc.indexHandle(event.did, event.time),
				]);
			} else if (event.$type === "com.atproto.sync.subscribeRepos#commit") {
				for (const op of event.ops) {
					const uri = AtUri.make(event.did, ...op.path.split("/"));
					const indexFn = op.action === "delete"
						? this.indexingSvc.deleteRecord(uri)
						: this.indexingSvc.indexRecord(
							uri,
							parseCid(op.cid),
							jsonToLex(op.record),
							op.action === "create" ? WriteOpAction.Create : WriteOpAction.Update,
							event.time,
						);
					this.background.add(() => this.indexingSvc.indexHandle(event.did, event.time));
					await Promise.all([
						indexFn,
						this.indexingSvc.setCommitLastSeen(
							event.did,
							parseCid(event.commit),
							event.rev,
						),
					]);
				}
			}
			return { success: true };
		} catch (err) {
			return { success: false, error: err };
		}
	}
}

function decodeChunk(chunk: Uint8Array): Event | undefined {
	const [header, remainder] = decodeFirst(chunk);
	const [body, remainder2] = decodeFirst(remainder);
	if (remainder2.length > 0) {
		throw new Error("excess bytes in message");
	}

	const { t, op } = parseHeader(header);

	if (op === -1) {
		throw new Error(`error: ${body.message}\nerror code: ${body.error}`);
	}

	if (t === "#commit") {
		const {
			seq,
			repo,
			commit,
			rev,
			blocks: blocksBytes,
			ops: commitOps,
			time,
		} = body as ComAtprotoSyncSubscribeRepos.Commit;

		if (!blocksBytes?.$bytes?.length) return;

		const blocks = fromBytes(blocksBytes);
		if (!blocks?.length) return;

		const car = readCar(blocks);

		const ops: Array<RepoOp> = [];
		for (const op of commitOps) {
			const action: "create" | "update" | "delete" = op.action as any;
			if (action === "create") {
				if (!op.cid) continue;
				const record = car.get(op.cid.$link);
				if (!record) continue;
				ops.push({
					action,
					path: op.path,
					cid: op.cid.$link,
					record,
				});
			} else if (action === "update") {
				if (!op.cid) continue;
				const record = car.get(op.cid.$link);
				if (!record) continue;
				ops.push({
					action,
					path: op.path,
					cid: op.cid.$link,
					...(op.prev ? { prev: op.prev.$link } : {}),
					record,
				});
			} else if (action === "delete") {
				ops.push({
					action,
					path: op.path,
					...(op.prev ? { prev: op.prev.$link } : {}),
				});
			} else {
				throw new Error(`Unknown action: ${action}`);
			}
		}

		return {
			$type: "com.atproto.sync.subscribeRepos#commit",
			seq,
			did: repo,
			commit: commit.$link,
			rev,
			ops,
			time,
		};
	} else if (t === "#sync") {
		const {
			seq,
			did,
			blocks: blocksBytes,
			rev,
			time,
		} = body as ComAtprotoSyncSubscribeRepos.Sync;

		if (!blocksBytes?.$bytes?.length) return;
		const blocks = fromBytes(blocksBytes);

		return {
			$type: "com.atproto.sync.subscribeRepos#sync",
			seq,
			did,
			blocks,
			rev,
			time,
		};
	} else if (t === "#account" || t === "#identity") {
		return {
			$type: `com.atproto.sync.subscribeRepos${t}`,
			...body,
		};
	} else {
		console.warn(`unknown message type ${t} ${body}`);
	}
}

function parseHeader(header: any): { t: string; op: 1 | -1 } {
	if (
		!header ||
		typeof header !== "object" ||
		!header.t ||
		typeof header.t !== "string" ||
		!header.op ||
		typeof header.op !== "number"
	) {
		throw new Error("invalid header received");
	}
	return { t: header.t, op: header.op };
}

function readCar(buffer: Uint8Array): Map<string, unknown> {
	const records = new Map<string, unknown>();
	for (const { cid, bytes } of iterateCar(buffer).iterate()) {
		records.set(toCidLink(cid).$link, decode(bytes));
	}
	return records;
}

function parseCid(
	cid: { $link: string } | { bytes: Uint8Array } | CID | string,
) {
	if (cid instanceof CID) {
		return cid;
	} else if (typeof cid === "string") {
		return CID.parse(cid);
	} else if ("$link" in cid) {
		return CID.parse(cid.$link);
	} else if ("bytes" in cid) {
		return CID.decode(cid.bytes);
	}
	throw new Error("invalid CID " + JSON.stringify(cid));
}

function jsonToLex(val: Record<string, unknown>): unknown {
	try {
		// walk arrays
		if (Array.isArray(val)) {
			return val.map((item) => jsonToLex(item));
		}
		// objects
		if (val && typeof val === "object") {
			// check for dag json values
			if (
				"$link" in val &&
				typeof val["$link"] === "string" &&
				Object.keys(val).length === 1
			) {
				return CID.parse(val["$link"]);
			}
			if ("bytes" in val && val["bytes"] instanceof Uint8Array) {
				return CID.decode(val.bytes);
			}
			if (
				val["$type"] === "blob" ||
				(typeof val["cid"] === "string" && typeof val["mimeType"] === "string")
			) {
				if ("ref" in val && typeof val["size"] === "number") {
					return new BlobRef(
						CID.decode((val.ref as any).bytes),
						val.mimeType as string,
						val.size,
					);
				} else {
					return new BlobRef(
						CID.parse(val.cid as string),
						val.mimeType as string,
						-1,
						val as never,
					);
				}
			}
			// walk plain objects
			const toReturn: Record<string, unknown> = {};
			for (const key of Object.keys(val)) {
				// @ts-expect-error — indexed access
				toReturn[key] = jsonToLex(val[key]);
			}
			return toReturn;
		}
	} catch {
		// pass through
	}
	return val;
}

type RepoOp =
	| {
		action: "create" | "update";
		path: string;
		cid: string;
		record: {};
	}
	| { action: "delete"; path: string };

type Event =
	| {
		$type: "com.atproto.sync.subscribeRepos#identity";
		did: string;
		seq: number;
		time: string;
	}
	| {
		$type: "com.atproto.sync.subscribeRepos#account";
		did: string;
		seq: number;
		active: boolean;
		status?: string;
	}
	| {
		$type: "com.atproto.sync.subscribeRepos#sync";
		did: string;
		seq: number;
		blocks: Uint8Array;
		rev: string;
		time: string;
	}
	| {
		$type: "com.atproto.sync.subscribeRepos#commit";
		did: string;
		seq: number;
		commit: string;
		rev: string;
		time: string;
		ops: Array<RepoOp>;
	};

export default new Worker();
