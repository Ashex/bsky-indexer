import { availableParallelism } from "node:os";
import { URL } from "node:url";
import { createClient, type RedisClientOptions } from "@redis/client";
import { WebSocket } from "partysocket";
import { DynamicThreadPool } from "@poolifier/poolifier-web-worker";
import type { PgOptions } from "@zeppelin-social/bsky/dist/data-plane/server/db/types";
import type { IdentityResolverOpts } from "@atproto/identity";
import { FirehoseSubscriptionError, FirehoseWorkerError } from "./errors.ts";
import type { WorkerInput, WorkerOutput, WorkerStartupMessage } from "./worker.ts";

let messagesReceived = 0,
	messagesProcessed = 0;

const MAX_ACCEPTABLE_QUEUE_SIZE = 100_000;

const DEFAULT_WORKER_URL = new URL("./bin/defaultWorker.ts", import.meta.url);

export class FirehoseSubscription extends DynamicThreadPool<WorkerInput, WorkerOutput> {
	private REDIS_SEQ_KEY = "bsky_indexer:seq";

	protected firehose!: WebSocket;
	protected redis?: ReturnType<typeof createClient>;

	protected cursor = "";
	protected logStatsInterval: number | null = null;
	protected saveCursorInterval: number | null = null;

	protected timings = {
		queued: { total: 0, count: 0 },
		index: { total: 0, count: 0 },
	};

	protected settings: FirehoseSubscriptionSettings;

	constructor(
		protected subOpts: FirehoseSubscriptionOptions,
		worker: URL = DEFAULT_WORKER_URL,
	) {
		const settings: FirehoseSubscriptionSettings = {
			minWorkers: subOpts.minWorkers ?? clamp(availableParallelism() / 2, 16, 32),
			maxWorkers: subOpts.maxWorkers ?? clamp(availableParallelism() * 2, 32, 64),
			maxConcurrency: subOpts.maxConcurrency ?? 100,
			maxTimestampDeltaMs: subOpts.maxTimestampDeltaMs,
			statsFrequencyMs: subOpts.statsFrequencyMs ?? 30_000,
		};

		super(
			settings.minWorkers,
			settings.maxWorkers,
			worker,
			{
				startWorkers: false,
				enableTasksQueue: true,
				workerChoiceStrategy: "INTERLEAVED_WEIGHTED_ROUND_ROBIN",
				tasksQueueOptions: {
					concurrency: settings.maxConcurrency,
					size: 1000,
				},
				workerOptions: {
					type: "module",
					deno: {
						permissions: "inherit",
					},
				},
			},
		);

		this.settings = settings;

		if (this.subOpts.redisOptions) {
			this.redis = createClient(this.subOpts.redisOptions);
		}
	}

	override async start(): Promise<void> {
		super.start();
		if (this.redis && !this.redis.isOpen) await this.redis.connect();

		const initialCursor = this.redis ? await this.redis.get(this.REDIS_SEQ_KEY) : null;

		if (this.cursor) {
			console.log(`starting from known cursor: ${this.cursor}`);
		} else if (initialCursor) {
			console.log(`starting from redis cursor: ${initialCursor}`);
			this.cursor = initialCursor;
		} else if (this.subOpts.cursor !== undefined) {
			console.log(`starting from provided cursor: ${this.subOpts.cursor}`);
			this.cursor = this.subOpts.cursor.toString();
		} else console.log(`starting from latest`);

		this.initFirehose();

		if (this.redis) {
			this.saveCursorInterval ??= setInterval(() => {
				if (messagesProcessed > 0 && messagesReceived > 0) {
					void this.redis!.set(this.REDIS_SEQ_KEY, this.cursor);
				}
			}, 55_000); // slightly offset from the 60s default log interval so the if condition isn't always false
		} else {
			console.warn("redis not configured, skipping cursor persistence");
		}

		if (!this.logStatsInterval && this.settings.statsFrequencyMs) {
			const secFreq = this.settings.statsFrequencyMs / 1000;
			this.logStatsInterval = setInterval(() => {
				if (messagesReceived === 0) return this.firehose.reconnect();

				if (this.info.queuedTasks && this.info.queuedTasks > MAX_ACCEPTABLE_QUEUE_SIZE) {
					console.warn(`queue size ${this.info.queuedTasks} exceeded max size, reconnecting in 30s`);
					this.firehose.close();
					setTimeout(() => this.initFirehose(this.cursor), 30_000);
					return;
				}

				const timings = {
					queued: this.timings.queued.total / this.timings.queued.count || 0,
					index: this.timings.index.total / this.timings.index.count || 0,
				};

				console.log(
					`${Math.round(messagesProcessed / secFreq)} / ${
						Math.round(
							messagesReceived / secFreq,
						)
					} per sec (${
						Math.round(
							(messagesProcessed / messagesReceived) * 100,
						)
					}%) [${this.info.workerNodes} workers; ${this.info.queuedTasks} queued; ${this.info.executingTasks} executing] {${this.cursor}}
avg timings (ms): queued=${timings.queued.toFixed(0)}, index=${timings.index.toFixed(0)}`,
				);

				messagesReceived = messagesProcessed = 0;
				this.timings = {
					queued: { total: 0, count: 0 },
					index: { total: 0, count: 0 },
				};
			}, this.settings.statsFrequencyMs);
		}
	}

	protected initFirehose(cursor?: string): void {
		this.firehose = new WebSocket(
			() => `${this.subOpts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${cursor ?? this.cursor}`,
		);
		this.firehose.binaryType = "arraybuffer";
		this.firehose.onmessage = this.onMessage;
		this.firehose.onerror = (e) => this.subOpts.onError?.(new FirehoseSubscriptionError(e.error));
	}

	protected onMessage = async (e: MessageEvent<ArrayBuffer>): Promise<void> => {
		const chunk = new Uint8Array(e.data);
		messagesReceived++;
		try {
			const res = await this.execute({ chunk, receivedAt: Date.now() }, undefined, [chunk.buffer]);
			this.onProcessed(res);
		} catch (e) {
			this.subOpts.onError?.(new FirehoseSubscriptionError(e));
		}
	};

	protected onProcessed = (res: WorkerOutput): void => {
		if (res?.success) {
			messagesProcessed++;
		} else if (res?.error) {
			this.subOpts.onError?.(new FirehoseWorkerError(res.error));
		}
		if (res?.cursor && !isNaN(res.cursor)) {
			this.cursor = `${res.cursor}`;
		}
		if (res?.timings) {
			this.timings.queued.total += res.timings.queuedMs;
			this.timings.queued.count++;
			this.timings.index.total += res.timings.indexMs;
			this.timings.index.count++;
		}
	};

	// https://github.com/poolifier/poolifier-web-worker/blob/3d8a51cbec22da21ec6450346fd44c598e618572/src/pools/thread/fixed.ts#L61
	protected override sendStartupMessageToWorker(workerNodeKey: number): void {
		const workerNode = this.workerNodes[workerNodeKey];
		const port2 = workerNode.messageChannel!.port2;
		workerNode.worker.postMessage(
			{
				ready: false,
				workerId: this.getWorkerInfo(workerNodeKey)?.id,
				port: port2,
				options: {
					dbOptions: this.subOpts.dbOptions,
					idResolverOptions: this.subOpts.idResolverOptions ?? {},
					maxTimestampDeltaMs: this.subOpts.maxTimestampDeltaMs,
				} satisfies WorkerStartupMessage["data"]["options"],
			},
			[port2],
		);
	}

	override async destroy(): Promise<void> {
		console.warn("destroying indexer");
		await super.destroy();
		await this?.redis?.quit();
	}
}

export interface FirehoseSubscriptionSettings {
	minWorkers: number;
	maxWorkers: number;
	maxConcurrency: number;
	maxTimestampDeltaMs?: number;
	statsFrequencyMs: number;
}

export interface FirehoseSubscriptionOptions extends Partial<FirehoseSubscriptionSettings> {
	service: string;
	dbOptions: PgOptions;
	redisOptions?: RedisClientOptions;
	idResolverOptions?: IdentityResolverOpts;
	onError?: (err: Error) => void;
	cursor?: number;
}

export type RepoOp =
	| {
		action: "create" | "update";
		path: string;
		cid: string;
		record: {};
	}
	| { action: "delete"; path: string };

export type Event =
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

function clamp(value: number, min: number, max: number): number {
	if (value < min) return min;
	if (value > max) return max;
	return value;
}
