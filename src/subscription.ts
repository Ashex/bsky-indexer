import { availableParallelism } from "node:os";
import { URL } from "node:url";
import { createClient, type RedisClientOptions } from "@redis/client";
import { WebSocket } from "partysocket";
import { DynamicThreadPool } from "@poolifier/poolifier-web-worker";
import type { PgOptions } from "@futuristick/atproto-bsky/dist/data-plane/server/db/types";
import type { IdentityResolverOpts } from "@atproto/identity";
import { FirehoseSubscriptionError, FirehoseWorkerError } from "./errors.ts";
import type { WorkerInput, WorkerOutput } from "./worker.ts";

let messagesReceived = 0,
	messagesProcessed = 0;

const DEFAULT_WORKER_URL = new URL("./bin/defaultWorker.ts", import.meta.url);

export class FirehoseSubscription extends DynamicThreadPool<WorkerInput, WorkerOutput> {
	private REDIS_SEQ_KEY = "bsky_indexer:seq";

	protected firehose!: WebSocket;
	protected redis?: ReturnType<typeof createClient>;

	protected cursor = "";
	protected logStatsInterval: number | null = null;
	protected saveCursorInterval: number | null = null;

	protected settings: FirehoseSubscriptionSettings;

	constructor(
		protected subOpts: FirehoseSubscriptionOptions,
		worker: URL = DEFAULT_WORKER_URL,
	) {
		const settings: FirehoseSubscriptionSettings = {
			minWorkers: subOpts.minWorkers ?? clamp(availableParallelism() / 2, 16, 32),
			maxWorkers: subOpts.maxWorkers ?? clamp(availableParallelism() * 2, 32, 64),
			maxConcurrency: subOpts.maxConcurrency ?? 75,
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
		} else if (this.subOpts.cursor) {
			console.log(`starting from provided cursor: ${this.subOpts.cursor}`);
			this.cursor = this.subOpts.cursor.toString();
		} else console.log(`starting from latest`);

		this.initFirehose();

		if (!this.logStatsInterval && this.settings.statsFrequencyMs) {
			const secFreq = this.settings.statsFrequencyMs / 1000;
			this.logStatsInterval = setInterval(() => {
				if (messagesReceived === 0) return this.firehose.reconnect();
				console.log(
					`${Math.round(messagesProcessed / secFreq)} / ${
						Math.round(
							messagesReceived / secFreq,
						)
					} per sec (${
						Math.round(
							(messagesProcessed / messagesReceived) * 100,
						)
					}%) [${this.info.workerNodes} workers; ${this.info.queuedTasks} queued; ${this.info.executingTasks} executing] {${this.cursor}}`,
				);
				messagesReceived = messagesProcessed = 0;
			}, this.settings.statsFrequencyMs);
		}

		if (this.redis && !this.saveCursorInterval) {
			this.saveCursorInterval = setInterval(async () => {
				if (messagesProcessed > 0 && messagesReceived > 0) {
					await this.redis!.set(this.REDIS_SEQ_KEY, this.cursor);
				}
			}, 60_000);
		}
	}

	protected initFirehose(): void {
		this.firehose = new WebSocket(
			() => `${this.subOpts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${this.cursor}`,
		);
		this.firehose.binaryType = "arraybuffer";
		this.firehose.onmessage = this.onMessage;
		this.firehose.onerror = (e) => this.subOpts.onError?.(new FirehoseSubscriptionError(e.error));
	}

	protected onMessage = async (e: MessageEvent<ArrayBuffer>): Promise<void> => {
		const chunk = new Uint8Array(e.data);
		messagesReceived++;
		try {
			const res = await this.execute({ chunk }, undefined, [chunk.buffer]);
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
					idResolverOptions: this.subOpts.idResolverOptions,
				},
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
