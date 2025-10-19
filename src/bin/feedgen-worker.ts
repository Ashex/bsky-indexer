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
import type { Event, RepoOp } from "../subscription.ts";
import { CustomIndexingService } from "../indexingService.ts";
import { FeedGeneratorFetcher } from "../feedgen-fetcher.ts";

// If indexing an event takes longer than this, we should clear the background queue before continuing
const MAX_INDEX_TIME_MS = 3000;

// Collections to process - only feed generator related records
const ALLOWED_COLLECTIONS = new Set([
	"app.bsky.feed.generator",
	"app.bsky.feed.threadgate",
]);

export type WorkerStartupMessage = MessageEvent<MessageValue<WorkerInput>> & {
	data: {
		options: {
			dbOptions: PgOptions;
			idResolverOptions: IdentityResolverOpts;
			maxTimestampDeltaMs?: number;
			feedgenAutoDiscovery?: boolean;
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

class FeedGenWorker extends ThreadWorker<WorkerInput, WorkerOutput> {
	db!: Database;
	idResolver!: IdResolver;
	background!: BackgroundQueue;
	indexingSvc!: CustomIndexingService;
	feedGenFetcher!: FeedGeneratorFetcher;
	
	// Track discovered service DIDs to avoid redundant queries
	private discoveredServiceDids = new Set<string>();
	// Debounce discovery to avoid overwhelming Constellation API
	private discoveryQueue = new Set<string>();
	private discoveryTimer: ReturnType<typeof setTimeout> | null = null;
	private readonly DISCOVERY_DEBOUNCE_MS = 5000; // Wait 5s before triggering discovery
	
	// Feature flag for auto-discovery
	private autoDiscoveryEnabled: boolean = false;

	constructor() {
		// must be async; poolifier uses that to determine whether to await
		super(async (data: WorkerInput | undefined) => this.process(data!), {
			maxInactiveTime: 60_000,
		});
		this.feedGenFetcher = new FeedGeneratorFetcher();
	}

	process = async ({ chunk, receivedAt }: WorkerInput): Promise<WorkerOutput> => {
		const queuedMs = Date.now() - receivedAt;
		try {
			const event = decodeChunk(chunk);
			if (!event) return { success: true, timings: { queuedMs, indexMs: 0 } };

			// Early exit: skip non-commit events entirely for feed gen indexer
			if (event.$type !== "com.atproto.sync.subscribeRepos#commit") {
				return { 
					success: true, 
					cursor: event.seq, 
					timings: { queuedMs, indexMs: 0 } 
				};
			}

			// Filter events - only process commits with allowed collections
			const hasRelevantOp = event.ops?.some(op => {
				if (!op || !op.path) return false;
				const collection = op.path.split("/")[0];
				return ALLOWED_COLLECTIONS.has(collection);
			});

			if (!hasRelevantOp) {
				// Skip this event, but still update cursor (no logging for filtered events)
				return { 
					success: true, 
					cursor: event.seq, 
					timings: { queuedMs, indexMs: 0 } 
				};
			}

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
			const collection = event.ops?.[0]?.action === "create"
				? event.ops?.[0]?.path?.split("/")[0]
				: undefined;

			if (success) {
				return { success, cursor: event.seq, collection, timings };
			} else {
				return { success, error, collection, timings };
			}
		} catch (err) {
			const error = err instanceof Error ? err.message : `${err}`;
			return { success: false, error, timings: { queuedMs, indexMs: 0 } };
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
				const roots = iterateCar(event.blocks).header.data.roots;
				if (!roots || !roots[0]) {
					console.warn(`[Feedgen Worker] Missing roots in sync event, skipping`);
					return { success: true };
				}
				const cid = parseCid(roots[0]);
				await Promise.all([
					this.indexingSvc.setCommitLastSeen(event.did, cid, event.rev),
					this.indexingSvc.indexHandle(event.did, event.time),
				]);
			} else if (event.$type === "com.atproto.sync.subscribeRepos#commit") {
				// For feed gen indexer, only process feed generation operations
				// No need to track commit last seen or handle identities for this specialized indexer
				await (async () => {
					for (const op of event.ops) {
						// Validate op has required fields
						if (!op || !op.path) {
							continue;
						}

						const uri = AtUri.make(event.did, ...op.path.split("/"));
						const collection = uri.collection;

						// Only process allowed collections
						if (!ALLOWED_COLLECTIONS.has(collection)) {
							continue;
						}

						// Log only feed generation activity
						const action = op.action === "delete" ? "delete" : (op.action === "create" ? "create" : "update");
						console.log(`[Feed Gen] Processing ${action} for ${uri.toString()}`);

						if (op.action === "delete") {
							await this.indexingSvc.deleteRecord(uri);
						} else {
							// Skip if CID is missing
							if (!op.cid) {
								console.warn(`[Feed Gen] Missing CID for ${uri.toString()}, skipping`);
								continue;
							}
							await this.indexingSvc.indexRecord(
								uri,
								parseCid(op.cid),
								jsonToLex(op.record),
								op.action === "create" ? WriteOpAction.Create : WriteOpAction.Update,
								event.time,
							);
							
							// If this is a new feed generator creation, trigger discovery for its service DID
							if (this.autoDiscoveryEnabled && op.action === "create" && collection === "app.bsky.feed.generator") {
								const record = jsonToLex(op.record) as any;
								if (record?.did) {
									this.queueDiscovery(record.did);
								}
							}
						}
					}
				})();
				return {
					success: true,
				};
			}
			return { success: true };
		} catch (error) {
			const errorMsg = error instanceof Error ? error.message : String(error);
			const stack = error instanceof Error ? error.stack : '';
			console.error(`[Feedgen Worker] Error in indexEvent:`, errorMsg);
			if (stack) console.error(`[Feedgen Worker] Stack:`, stack);
			return { success: false, error: errorMsg };
		}
	}

	/**
	 * Queue a service DID for discovery (with debouncing)
	 */
	private queueDiscovery(serviceDid: string): void {
		// Skip if already discovered recently
		if (this.discoveredServiceDids.has(serviceDid)) {
			return;
		}

		// Add to queue
		this.discoveryQueue.add(serviceDid);
		
		// Reset debounce timer
		if (this.discoveryTimer) {
			clearTimeout(this.discoveryTimer);
		}
		
		// Schedule discovery
		this.discoveryTimer = setTimeout(() => {
			this.performDiscovery().catch(err => {
				console.error("[Feed Gen] Discovery error:", err);
			});
		}, this.DISCOVERY_DEBOUNCE_MS);
	}

	/**
	 * Perform Constellation discovery for queued service DIDs
	 */
	private async performDiscovery(): Promise<void> {
		if (this.discoveryQueue.size === 0) {
			return;
		}

		const serviceDids = Array.from(this.discoveryQueue);
		this.discoveryQueue.clear();

		console.log(`[Feed Gen Discovery] Discovering feeds for ${serviceDids.length} service DID(s)...`);

		let totalDiscovered = 0;
		let totalIndexed = 0;

		for (const serviceDid of serviceDids) {
			try {
				// Mark as discovered to prevent redundant queries
				this.discoveredServiceDids.add(serviceDid);

				// Discover feeds for this service
				const uris = await this.feedGenFetcher.discoverFeedGeneratorsByService(serviceDid);
				
				if (uris.length === 0) {
					console.log(`[Feed Gen Discovery] No additional feeds found for ${serviceDid}`);
					continue;
				}

				console.log(`[Feed Gen Discovery] Found ${uris.length} feeds for ${serviceDid}, fetching and indexing...`);
				totalDiscovered += uris.length;

				// Fetch and index each feed
				for (const uri of uris) {
					try {
						const record = await this.feedGenFetcher.fetchFeedGenerator(uri);
						
						if (!record) {
							continue;
						}

						// Index the feed generator
						const success = await this.indexFeedGenerator(record);
						if (success) {
							totalIndexed++;
						}

						// Small delay to avoid overwhelming the system
						await new Promise(resolve => setTimeout(resolve, 50));
					} catch (err) {
						console.error(`[Feed Gen Discovery] Error fetching/indexing ${uri}:`, err);
					}
				}
			} catch (err) {
				console.error(`[Feed Gen Discovery] Error discovering feeds for ${serviceDid}:`, err);
			}
		}

		if (totalDiscovered > 0) {
			console.log(`[Feed Gen Discovery] Completed: ${totalIndexed}/${totalDiscovered} feeds indexed`);
		}
	}

	/**
	 * Index a feed generator record (similar to FeedGeneratorIndexer)
	 */
	private async indexFeedGenerator(record: any): Promise<boolean> {
		try {
			const uri = record.uri;
			const parts = uri.replace("at://", "").split("/");
			const creator = parts[0];
			
			// Extract avatar CID if present
			let avatarCid: string | null = null;
			if (record.value.avatar) {
				if (typeof record.value.avatar === "object" && "$link" in record.value.avatar) {
					avatarCid = (record.value.avatar as { $link: string }).$link;
				} else if (typeof record.value.avatar === "string") {
					avatarCid = record.value.avatar;
				}
			}

			const now = new Date().toISOString();

			await this.db.db
				.insertInto("feed_generator")
				.values({
					uri,
					cid: record.cid,
					creator,
					feedDid: record.value.did,
					displayName: record.value.displayName,
					description: record.value.description || null,
					avatarCid: avatarCid,
					createdAt: record.value.createdAt,
					indexedAt: now,
				})
				.onConflict((oc) => 
					oc.column("uri").doUpdateSet({
						cid: record.cid,
						feedDid: record.value.did,
						displayName: record.value.displayName,
						description: record.value.description || null,
						avatarCid: avatarCid,
						indexedAt: now,
					})
				)
				.execute();

			console.log(`[Feed Gen Discovery] Indexed: ${record.value.displayName}`);
			return true;
		} catch (err) {
			console.error(`[Feed Gen Discovery] Error indexing ${record.uri}:`, err);
			return false;
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

		// Configure auto-discovery from options
		this.autoDiscoveryEnabled = messageEvent.data?.options?.feedgenAutoDiscovery || false;
		
		if (this.autoDiscoveryEnabled) {
			console.log("[Feed Gen Worker] Auto-discovery enabled - will discover related feeds when new feeds are indexed");
		} else {
			console.log("[Feed Gen Worker] Auto-discovery disabled - set FEEDGEN_AUTO_DISCOVERY=true to enable");
		}

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
		try {
			// Early filtering: Only process allowed collections
			const collection = op.path.split('/')[0];
			if (!ALLOWED_COLLECTIONS.has(collection)) {
				continue;
			}

			const action: "create" | "update" | "delete" = op.action as any;
			if (action === "create") {
				if (!op.cid) {
					console.debug(`[Feedgen Decode] Skipping create with no CID: ${op.path}`);
					continue;
				}
				if (!op.cid.$link) {
					console.warn(`[Feedgen Decode] CID exists but no $link property. CID type: ${typeof op.cid}, value: ${JSON.stringify(op.cid)}`);
					continue;
				}
				const record = car.get(op.cid.$link);
				if (!record) {
					console.warn(`[Feed Gen] Record not found in CAR for feed generator: ${op.path}`);
					continue;
				}
				ops.push({
					action,
					path: op.path,
					cid: op.cid.$link,
					record,
				});
			} else if (action === "update") {
				if (!op.cid) {
					console.debug(`[Feedgen Decode] Skipping update with no CID: ${op.path}`);
					continue;
				}
				if (!op.cid.$link) {
					console.warn(`[Feedgen Decode] CID exists but no $link property. CID type: ${typeof op.cid}, value: ${JSON.stringify(op.cid)}`);
					continue;
				}
				const record = car.get(op.cid.$link);
				if (!record) {
					console.warn(`[Feed Gen] Record not found in CAR for feed generator: ${op.path}`);
					continue;
				}
				ops.push({
					action,
					path: op.path,
					cid: op.cid.$link,
					record,
				});
			} else {
				ops.push({
					action,
					path: op.path,
				});
			}
		} catch (err) {
			console.error(`[Feedgen Decode] Error processing op:`, err);
			continue;
		}
	}		return {
			$type: "com.atproto.sync.subscribeRepos#commit",
			did: repo,
			seq,
			commit: commit.$link,
			rev,
			time,
			ops,
		};
	}

	if (t === "#i	docker-compose exec postgres psql -U postgres -d postgres -c "SELECT * FROM bsky.feed_generator ORDER BY indexed_at DESC LIMIT 20;"tity") {
		const { seq, did, time } = body as ComAtprotoSyncSubscribeRepos.Identity;
		return {
			$type: "com.atproto.sync.subscribeRepos#identity",
			did,
			seq,
			time,
		};
	}

	if (t === "#account") {
		const { seq, did, active, status } = body as ComAtprotoSyncSubscribeRepos.Account;
		return {
			$type: "com.atproto.sync.subscribeRepos#account",
			did,
			seq,
			active,
			status,
		};
	}

	if (t === "#handle") {
		const { seq, did, handle, time } = body as ComAtprotoSyncSubscribeRepos.Handle;
		return {
			$type: "com.atproto.sync.subscribeRepos#identity",
			did,
			seq,
			time,
		};
	}

	if (t === "#migrate") {
		const { seq, did, migrateTo, time } = body as ComAtprotoSyncSubscribeRepos.Migrate;
		return {
			$type: "com.atproto.sync.subscribeRepos#sync",
		did: migrateTo || did,
		seq,
		blocks: new Uint8Array(),
		rev: "",
		time,
	};
}	if (t === "#tombstone") {
		const { seq, did, time } = body as ComAtprotoSyncSubscribeRepos.Tombstone;
		return {
			$type: "com.atproto.sync.subscribeRepos#account",
			did,
			seq,
			active: false,
			status: "deleted",
		};
	}

	if (t === "#info") {
		const { name, message } = body as ComAtprotoSyncSubscribeRepos.Info;
		console.info(`[Feedgen Indexer] Info from relay: ${name}: ${message}`);
		return;
	}

	console.debug(`[Feedgen Indexer] unknown event type: ${t}`);
}

function readCar(blocks: Uint8Array): Map<string, any> {
	const car = new Map();
	for (const entry of iterateCar(blocks)) {
		if (entry && entry.data && entry.data.cid) {
			car.set(entry.data.cid.toString(), decode(entry.data.bytes));
		}
	}
	return car;
}

function parseHeader(header: any): { t?: string; op?: number } {
	if (header.$type) {
		return {
			t: header.$type.split("#")[1],
			op: header.op,
		};
	}
	return {
		t: header.t,
		op: header.op,
	};
}

function parseCid(val: any): CID {
	if (!val) throw new Error("no cid");
	if (typeof val === "string") {
		return CID.parse(val);
	} else if (val.$link) {
		return CID.parse(val.$link);
	} else if (val instanceof Uint8Array) {
		return CID.decode(val);
	}
	throw new Error("no cid");
}

function jsonToLex(obj: unknown): unknown {
	if (Array.isArray(obj)) {
		return obj.map(jsonToLex);
	}
	if (obj && typeof obj === "object") {
		const toReturn: Record<string, unknown> = {};
		for (const [key, val] of Object.entries(obj)) {
			if (key === "$bytes") {
				return new Uint8Array(Object.values(val as Record<string, number>));
			}
			if (key === "$link") {
				return toCidLink(val as string);
			}
			if (key === "$type" && val === "blob") {
				return BlobRef.fromJsonRef(obj as any);
			}
			toReturn[key] = jsonToLex(val);
		}
		return toReturn;
	}
	return obj;
}

new FeedGenWorker();
