import { readCar as iterateCar } from "@atcute/car";
import { decode, decodeFirst, fromBytes, toCidLink } from "@atcute/cbor";
import type { ComAtprotoSyncSubscribeRepos } from "@atcute/atproto";
import { CID } from "multiformats/cid";
import { type MessageValue, ThreadWorker } from "@poolifier/poolifier-web-worker";
import { BackgroundQueue, Database } from "@zeppelin-social/bsky";
import type { PgOptions } from "@zeppelin-social/bsky/dist/data-plane/server/db/types";
import type { IdentityResolverOpts } from "@atproto/identity";
import { IdResolver, MemoryCache } from "@atproto/identity";
import { BlobRef } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import { AtUri } from "@atproto/syntax";
import type { Event, RepoOp } from "./subscription.ts";
import { CustomIndexingService } from "./indexingService.ts";

// If indexing an event takes longer than this, we should clear the background queue before continuing
const MAX_INDEX_TIME_MS = 3000;

export type WorkerStartupMessage = MessageEvent<MessageValue<WorkerInput>> & {
	data: {
		options: {
			dbOptions: PgOptions;
			idResolverOptions: IdentityResolverOpts;
			maxTimestampDeltaMs?: number;
		};
	};
};

export type WorkerInput = {
	chunk: Uint8Array;
	receivedAt: number;
};

export type WorkerOutput = {
	success?: boolean;
	cursor?: number;
	error?: unknown;
	collection?: string;
	timings?: {
		queuedMs: number;
		indexMs: number;
	};
};

class Worker extends ThreadWorker<WorkerInput, WorkerOutput> {
	db!: Database;
	idResolver!: IdResolver;
	background!: BackgroundQueue;
	indexingSvc!: CustomIndexingService;

	constructor() {
		// must be async; poolifier uses that to determine whether to await
		super(async (data: WorkerInput | undefined) => this.process(data!), {
			maxInactiveTime: 60_000,
		});
	}

	process = async ({ chunk, receivedAt }: WorkerInput): Promise<WorkerOutput> => {
		const queuedMs = Date.now() - receivedAt;
		try {
			const event = decodeChunk(chunk);
			if (!event) return { success: true, timings: { queuedMs, indexMs: 0 } };

			const indexStart = Date.now();
			const { success, error } = await this.indexEvent(event);
			const indexMs = Date.now() - indexStart;

			if (indexMs > MAX_INDEX_TIME_MS) {
				await this.background.processAll();
			}

			const timings = {
				queuedMs,
				indexMs,
			};
			const collection = event.$type === "com.atproto.sync.subscribeRepos#commit" &&
					event.ops?.[0]?.action === "create"
				? event.ops?.[0]?.path?.split("/")[0]
				: undefined;

			if (success) {
				return { success, cursor: event.seq, collection, timings };
			} else {
				return { success, error, collection, timings };
			}
		} catch (err) {
			return { success: false, error: err, timings: { queuedMs, indexMs: 0 } };
		}
	};

	async indexEvent(
		event: Event,
	): Promise<
		{ success: boolean; error?: unknown; avgIndexTime?: number }
	> {
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
				this.background.add(() => this.indexingSvc.indexHandle(event.did, event.time));
				this.background.add(() =>
					this.indexingSvc.setCommitLastSeen(
						event.did,
						parseCid(event.commit),
						event.rev,
					)
				);

				for (const op of event.ops) {
					const uri = AtUri.make(event.did, ...op.path.split("/"));
					if (op.action === "delete") {
						await this.indexingSvc.deleteRecord(uri);
					} else {
						await this.indexingSvc.indexRecord(
							uri,
							parseCid(op.cid),
							jsonToLex(op.record),
							op.action === "create" ? WriteOpAction.Create : WriteOpAction.Update,
							event.time,
						);
					}
				}
				return {
					success: true,
				};
			}
			return { success: true };
		} catch (err) {
			return { success: false, error: err };
		}
	}

	protected override handleReadyMessageEvent(
		messageEvent: WorkerStartupMessage,
	): void {
		if (!messageEvent.data?.options?.dbOptions || !messageEvent.data?.options?.idResolverOptions) {
			throw new Error("worker missing options");
		}

		this.db = new Database(messageEvent.data.options.dbOptions);
		this.idResolver = new IdResolver({
			...messageEvent.data.options.idResolverOptions,
			didCache: new MemoryCache(),
		});
		this.background = new BackgroundQueue(this.db);
		this.indexingSvc = new CustomIndexingService(
			this.db,
			this.idResolver,
			this.background,
			messageEvent.data?.options?.maxTimestampDeltaMs,
		);

		if (
			messageEvent.data?.workerId != null &&
			messageEvent.data?.ready === false &&
			messageEvent.data?.port != null
		) {
			try {
				this.id = messageEvent.data.workerId;
				// @ts-expect-error — port is private
				this.port = messageEvent.data.port;
				// @ts-expect-error — port is private
				this.port.onmessage = this.messageEventListener.bind(this);
				this.sendToMainWorker({
					ready: true,
					taskFunctionsProperties: this.listTaskFunctionsProperties(),
				});
			} catch {
				this.sendToMainWorker({
					ready: false,
					taskFunctionsProperties: this.listTaskFunctionsProperties(),
				});
			}
		}
	}
}

export function decodeChunk(chunk: Uint8Array): Event | undefined {
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

export function readCar(buffer: Uint8Array): Map<string, unknown> {
	const records = new Map<string, unknown>();
	for (const { cid, bytes } of iterateCar(buffer).iterate()) {
		records.set(toCidLink(cid).$link, decode(bytes));
	}
	return records;
}

export function parseCid(
	cid: { $link: string } | { bytes: Uint8Array } | CID | string,
): CID {
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

export function jsonToLex(val: Record<string, unknown>): unknown {
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
			if ("$bytes" in val && typeof val["$bytes"] === "string" && Object.keys(val).length === 1) {
				return fromBytes({ $bytes: val.$bytes });
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

export { Worker as IndexerWorker };
