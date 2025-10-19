/**
 * Feed Generator Indexer
 * 
 * Integrates FeedGeneratorFetcher with the database indexing system.
 * Handles discovery, fetching, and indexing of feed generators using
 * Constellation (discovery) and Slingshot (fetching) APIs.
 */

import type { Database } from "@zeppelin-social/bsky";
import { FeedGeneratorFetcher, type FeedGeneratorRecord } from "./feedgen-fetcher.ts";

export interface FeedGeneratorIndexerOptions {
	/** Database instance */
	db: Database;
	/** Known feed service DIDs (optional, uses defaults if not provided) */
	serviceDids?: string[];
	/** Batch size for processing */
	batchSize?: number;
	/** Enable verbose logging */
	verbose?: boolean;
}

export class FeedGeneratorIndexer {
	private fetcher: FeedGeneratorFetcher;
	private db: Database;
	private verbose: boolean;

	constructor(private options: FeedGeneratorIndexerOptions) {
		this.fetcher = new FeedGeneratorFetcher();
		this.db = options.db;
		this.verbose = options.verbose || false;
	}

	/**
	 * Get unique feed service DIDs from the database
	 * These are the DIDs that host feed generators
	 */
	async getKnownFeedServiceDids(): Promise<string[]> {
		try {
			const result = await this.db.db
				.selectFrom("feed_generator")
				.select("feedDid")
				.distinct()
				.where("feedDid", "is not", null)
				.orderBy("feedDid")
				.execute();
			
			return result.map(row => row.feedDid).filter((did): did is string => did !== null);
		} catch (err) {
			console.error("[FeedGen Indexer] Error fetching feed service DIDs from database:", err);
			return [];
		}
	}

	/**
	 * Index a feed generator record into the database
	 */
	async indexFeedGenerator(record: FeedGeneratorRecord): Promise<boolean> {
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

			if (this.verbose) {
				console.log(`[FeedGen Indexer] Indexed: ${record.value.displayName} (${uri})`);
			}

			return true;
		} catch (err) {
			console.error(`[FeedGen Indexer] Error indexing ${record.uri}:`, err);
			return false;
		}
	}

	/**
	 * Run a full backfill of feed generators
	 * 
	 * Discovers feed generators from known service DIDs (both from database and defaults),
	 * fetches their records, and indexes them into the database.
	 */
	async runBackfill(): Promise<{
		discovered: number;
		indexed: number;
		skipped: number;
		errors: number;
	}> {
		console.log("[FeedGen Indexer] Starting feed generator backfill...");

		// Get service DIDs from database
		const dbServiceDids = await this.getKnownFeedServiceDids();
		console.log(`[FeedGen Indexer] Found ${dbServiceDids.length} service DIDs in database`);

		// Combine with provided or default service DIDs
		const configuredDids = this.options.serviceDids || [];
		const allServiceDids = [...new Set([...dbServiceDids, ...configuredDids])];

		let indexedCount = 0;
		let errorCount = 0;

		// Run backfill with indexing callback
		const stats = await this.fetcher.backfillFeedGenerators({
			serviceDids: allServiceDids,
			batchSize: this.options.batchSize,
			onFeedDiscovered: async (record) => {
				const success = await this.indexFeedGenerator(record);
				if (success) {
					indexedCount++;
				} else {
					errorCount++;
				}
			},
			onProgress: (processed, total) => {
				if (processed % 100 === 0 || processed === total) {
					console.log(`[FeedGen Indexer] Progress: ${processed}/${total} (${indexedCount} indexed, ${errorCount} errors)`);
				}
			},
		});

		console.log("[FeedGen Indexer] Backfill complete!");
		console.log(`  - Discovered: ${stats.discovered}`);
		console.log(`  - Indexed: ${indexedCount}`);
		console.log(`  - Skipped: ${stats.skipped}`);
		console.log(`  - Errors: ${errorCount}`);

		return {
			discovered: stats.discovered,
			indexed: indexedCount,
			skipped: stats.skipped,
			errors: errorCount,
		};
	}

	/**
	 * Run periodic backfill updates
	 * 
	 * @param intervalMs Interval in milliseconds between backfill runs
	 * @returns Timer ID that can be used to cancel the periodic updates
	 */
	startPeriodicBackfill(intervalMs: number = 1000 * 60 * 60 * 6): number {
		console.log(`[FeedGen Indexer] Starting periodic backfill (interval: ${intervalMs}ms)`);
		
		// Run initial backfill
		this.runBackfill().catch((err) => {
			console.error("[FeedGen Indexer] Initial backfill failed:", err);
		});

		// Schedule periodic updates
		return setInterval(() => {
			this.runBackfill().catch((err) => {
				console.error("[FeedGen Indexer] Periodic backfill failed:", err);
			});
		}, intervalMs) as unknown as number;
	}
}
