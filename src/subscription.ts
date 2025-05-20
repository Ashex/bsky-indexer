import { readFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { URL } from "node:url";
import { Blob } from "node:buffer";
import { createClient, type RedisClientOptions } from "@redis/client";
import { WebSocket } from "partysocket";
import { DynamicThreadPool } from "@poolifier/poolifier-web-worker";
import type { PgOptions } from "@atproto/bsky/dist/data-plane/server/db/types";
import type { IdentityResolverOpts } from "@atproto/identity";
import { FirehoseSubscriptionError, FirehoseWorkerError } from "./errors.ts";
import type { WorkerInput, WorkerOutput } from "./worker.ts";

let messagesReceived = 0,
	messagesProcessed = 0;

const DEFAULT_WORKER_URL = new URL("./worker.ts", import.meta.url);

export class FirehoseSubscription {
	private REDIS_SEQ_KEY = "bsky_indexer:seq";

	protected firehose!: WebSocket;
	protected pool: DynamicThreadPool<WorkerInput, WorkerOutput>;
	protected redis?: ReturnType<typeof createClient>;

	protected cursor = "";
	protected logStatsInterval: number | null = null;
	protected saveCursorInterval: number | null = null;

	protected settings: FirehoseSubscriptionSettings = {
		minWorkers: clamp(availableParallelism() / 2, 16, 32),
		maxWorkers: clamp(availableParallelism() * 2, 32, 64),
		maxConcurrency: 75,
		statsFrequencyMs: 30_000,
	};

	constructor(
		protected opts: FirehoseSubscriptionOptions,
		worker: URL | Blob = DEFAULT_WORKER_URL,
	) {
		if (this.opts.minWorkers) this.settings.minWorkers = this.opts.minWorkers;
		if (this.opts.maxWorkers) this.settings.maxWorkers = this.opts.maxWorkers;
		if (this.opts.maxConcurrency) this.settings.maxConcurrency = this.opts.maxConcurrency;
		if (this.opts.statsFrequencyMs !== undefined) {
			this.settings.statsFrequencyMs = this.opts.statsFrequencyMs;
		}

		const { dbOptions, idResolverOptions } = this.opts;
		// hack to get options into the worker
		const workerBlob = worker instanceof Blob ? worker : new Blob(
			[
				readFileSync(worker),
				worker === DEFAULT_WORKER_URL
					? `\nexport default new Worker(${JSON.stringify({ dbOptions, idResolverOptions })});`
					: "",
			],
			{ type: "application/typescript" },
		);

		this.pool = new DynamicThreadPool(
			this.settings.minWorkers,
			this.settings.maxWorkers,
			new URL(URL.createObjectURL(workerBlob)),
			{
				enableTasksQueue: true,
				workerChoiceStrategy: "INTERLEAVED_WEIGHTED_ROUND_ROBIN",
				tasksQueueOptions: {
					concurrency: this.settings.maxConcurrency,
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

		if (this.opts.redisOptions) {
			this.redis = createClient(this.opts.redisOptions);
		}
	}

	async start(): Promise<void> {
		if (this.redis && !this.redis.isOpen) await this.redis.connect();

		const initialCursor = this.redis ? await this.redis.get(this.REDIS_SEQ_KEY) : null;

		if (this.cursor) {
			console.log(`starting from known cursor: ${this.cursor}`);
		} else if (initialCursor) {
			console.log(`starting from redis cursor: ${initialCursor}`);
			this.cursor = initialCursor;
		} else if (this.opts.cursor) {
			console.log(`starting from provided cursor: ${this.opts.cursor}`);
			this.cursor = this.opts.cursor.toString();
		} else console.log(`starting from latest`);

		this.initFirehose();

		if (!this.logStatsInterval && this.settings.statsFrequencyMs) {
			const secFreq = this.settings.statsFrequencyMs / 1000;
			this.logStatsInterval = setInterval(() => {
				console.log(
					`${Math.round(messagesProcessed / secFreq)} / ${
						Math.round(
							messagesReceived / secFreq,
						)
					} per sec (${
						Math.round(
							(messagesProcessed / messagesReceived) * 100,
						)
					}%) [${this.pool.info.workerNodes} workers; ${this.pool.info.queuedTasks} queued; ${this.pool.info.executingTasks} executing]`,
				);
				messagesReceived = messagesProcessed = 0;
			}, this.settings.statsFrequencyMs);
		}

		if (this.redis && !this.saveCursorInterval) {
			this.saveCursorInterval = setInterval(async () => {
				await this.redis!.set(this.REDIS_SEQ_KEY, this.cursor);
			}, 60_000);
		}
	}

	protected initFirehose(): void {
		this.firehose = new WebSocket(
			() => `${this.opts.service}/xrpc/com.atproto.sync.subscribeRepos?cursor=${this.cursor}`,
		);
		this.firehose.binaryType = "arraybuffer";
		this.firehose.onmessage = this.onMessage;
		this.firehose.onerror = (e) => this.opts.onError?.(new FirehoseSubscriptionError(e.error));
	}

	protected onMessage = async (e: MessageEvent<ArrayBuffer>): Promise<void> => {
		const chunk = new Uint8Array(e.data);
		messagesReceived++;
		try {
			const res = await this.pool
				.execute({ chunk }, undefined, [chunk.buffer]);
			this.onProcessed(res);
		} catch (e) {
			this.opts.onError?.(new FirehoseSubscriptionError(e));
		}
	};

	protected onProcessed = (res: WorkerOutput): void => {
		if (res?.success) {
			messagesProcessed++;
		} else if (res?.error) {
			this.opts.onError?.(new FirehoseWorkerError(res.error));
		}
		if (res?.cursor && !isNaN(res.cursor)) {
			this.cursor = `${res.cursor}`;
		}
	};

	async destroy(): Promise<void> {
		console.warn("destroying indexer");
		await this?.pool?.destroy();
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
