/**
 * Feed Generator Fetcher
 * 
 * Fetches feed generators via Constellation (discovery) and Slingshot (fetching).
 * Uses Constellation API to discover feed generator URIs by querying for records
 * that link to known feed service DIDs, then fetches actual records via Slingshot.
 * 
 * This is more efficient than consuming the entire bsky.network firehose.
 */

const SLINGSHOT_API = "https://slingshot.microcosm.blue";
const CONSTELLATION_API = "https://constellation.microcosm.blue";
const RATE_LIMIT_DELAY = 50; // ms between requests

export interface FeedGeneratorRecord {
	uri: string;
	cid: string;
	value: {
		$type: "app.bsky.feed.generator";
		did: string;
		displayName: string;
		description?: string;
		avatar?: string;
		createdAt: string;
		[key: string]: unknown;
	};
}

export interface SlingshotRecordResponse {
	uri: string;
	cid: string;
	value: Record<string, unknown>;
}

export interface SlingshotErrorResponse {
	error: string;
	message: string;
}

export interface ConstellationResponse {
	total: number;
	linking_records: Array<{
		did: string;
		collection: string;
		rkey: string;
	}>;
	cursor: string | null;
}

export interface FeedGeneratorDiscoveryOptions {
	/** Known feed service DIDs to query */
	serviceDids?: string[];
	/** Batch size for processing discovered feeds */
	batchSize?: number;
	/** Callback function to handle discovered feed generators */
	onFeedDiscovered?: (record: FeedGeneratorRecord) => Promise<void>;
	/** Callback function to handle progress */
	onProgress?: (processed: number, total: number) => void;
}

export class FeedGeneratorFetcher {
	private cache = new Map<string, number>(); // DID -> last check timestamp
	private cacheExpiry = 1000 * 60 * 60 * 24; // 24 hours

	// Default known feed service DIDs
	private static readonly DEFAULT_SERVICE_DIDS = [
		"did:web:discover.bsky.app",      // Official Bluesky discovery feeds
		"did:web:feed.atproto.blue",      // Community feeds (atproto.blue)
		"did:web:api.goodfeeds.co",       // Goodfeeds
		"did:plc:z72i7hdynmk6r22z27h6tvur", // Bluesky official feeds
	];

	/**
	 * List all feed generators for a given DID by querying their repo
	 */
	listUserFeedGenerators(did: string): string[] {
		// Check cache to avoid excessive requests
		const lastCheck = this.cache.get(did);
		if (lastCheck && Date.now() - lastCheck < this.cacheExpiry) {
			return [];
		}

		// Mark this DID as checked
		this.cache.set(did, Date.now());
		
		// Note: We can't list records without PDS access or firehose
		// This method is kept for future implementation
		return [];
	}

	/**
	 * Fetch a specific feed generator record via Slingshot
	 */
	async fetchFeedGenerator(uri: string): Promise<FeedGeneratorRecord | null> {
		try {
			const url = new URL("/xrpc/com.bad-example.repo.getUriRecord", SLINGSHOT_API);
			url.searchParams.set("at_uri", uri);

			const response = await fetch(url.toString(), {
				headers: {
					"Accept": "application/json",
				},
			});

			if (!response.ok) {
				if (response.status === 400) {
					const error = await response.json() as SlingshotErrorResponse;
					if (error.error === "RecordNotFound") {
						console.debug(`[FeedGen Fetcher] Record not found: ${uri}`);
						return null;
					}
				}
				console.warn(`[FeedGen Fetcher] HTTP ${response.status} fetching ${uri}`);
				return null;
			}

			const data = await response.json() as SlingshotRecordResponse;
			
			// Validate it's actually a feed generator
			if (data.value.$type !== "app.bsky.feed.generator") {
				console.warn(`[FeedGen Fetcher] Wrong type for ${uri}: ${data.value.$type}`);
				return null;
			}

			return {
				uri: data.uri,
				cid: data.cid,
				value: data.value as FeedGeneratorRecord["value"],
			};
		} catch (err) {
			console.error(`[FeedGen Fetcher] Error fetching ${uri}:`, err);
			return null;
		}
	}

	/**
	 * Fetch all feed generators for a DID and return records
	 */
	async fetchUserFeedGenerators(did: string, rkeys: string[]): Promise<FeedGeneratorRecord[]> {
		const generators: FeedGeneratorRecord[] = [];

		for (const rkey of rkeys) {
			const uri = `at://${did}/app.bsky.feed.generator/${rkey}`;
			const record = await this.fetchFeedGenerator(uri);
			if (record) {
				generators.push(record);
			}
		}

		return generators;
	}

	/**
	 * Check if a DID was recently checked
	 */
	wasRecentlyChecked(did: string): boolean {
		const lastCheck = this.cache.get(did);
		return lastCheck ? Date.now() - lastCheck < this.cacheExpiry : false;
	}

	/**
	 * Mark a DID as checked
	 */
	markAsChecked(did: string): void {
		this.cache.set(did, Date.now());
	}

	/**
	 * Discover feed generator URIs using Constellation API for a specific service DID
	 * 
	 * Queries Constellation for all app.bsky.feed.generator records where the .did
	 * field points to the specified service DID.
	 */
	async discoverFeedGeneratorsByService(serviceDid: string): Promise<string[]> {
		try {
			const url = new URL("/links", CONSTELLATION_API);
			url.searchParams.set("target", serviceDid);
			url.searchParams.set("collection", "app.bsky.feed.generator");
			url.searchParams.set("path", ".did");
			url.searchParams.set("limit", "100");

			const uris: string[] = [];
			let cursor: string | null = null;

			// Paginate through all results
			do {
				if (cursor) {
					url.searchParams.set("cursor", cursor);
				}

				const response = await fetch(url.toString(), {
					headers: { "Accept": "application/json" },
					signal: AbortSignal.timeout(10000),
				});

				if (!response.ok) {
					if (response.status !== 404) {
						console.warn(`[FeedGen Fetcher] HTTP ${response.status} from Constellation for service ${serviceDid}`);
					}
					break;
				}

				const data = await response.json() as ConstellationResponse;

				// Validate response structure
				if (!data.linking_records || !Array.isArray(data.linking_records)) {
					console.warn(`[FeedGen Fetcher] Invalid response structure from Constellation for service ${serviceDid}`);
					break;
				}

				// Convert linking_records to AT-URIs
				for (const record of data.linking_records) {
					const uri = `at://${record.did}/${record.collection}/${record.rkey}`;
					uris.push(uri);
				}

				cursor = data.cursor || null;
			} while (cursor);

			return uris;
		} catch (err) {
			console.error(`[FeedGen Fetcher] Error discovering generators for service ${serviceDid}:`, err);
			return [];
		}
	}

	/**
	 * Discover all feed generators across known service providers
	 * 
	 * @param options Discovery options including service DIDs and callbacks
	 * @returns Array of discovered feed generator URIs
	 */
	async discoverAllFeedGenerators(options: FeedGeneratorDiscoveryOptions = {}): Promise<string[]> {
		const serviceDids = options.serviceDids || FeedGeneratorFetcher.DEFAULT_SERVICE_DIDS;
		const allUris = new Set<string>();

		console.log(`[FeedGen Fetcher] Discovering feed generators from ${serviceDids.length} service DIDs...`);

		for (const serviceDid of serviceDids) {
			console.log(`[FeedGen Fetcher] Querying feeds for service: ${serviceDid}`);
			const uris = await this.discoverFeedGeneratorsByService(serviceDid);
			console.log(`[FeedGen Fetcher] Found ${uris.length} feeds for ${serviceDid}`);
			
			for (const uri of uris) {
				allUris.add(uri);
			}
			
			// Small delay between services to avoid overwhelming the API
			await new Promise(resolve => setTimeout(resolve, 500));
		}

		console.log(`[FeedGen Fetcher] Total unique feed generators discovered: ${allUris.size}`);
		return Array.from(allUris);
	}

	/**
	 * Backfill feed generators by discovering via Constellation and fetching via Slingshot
	 * 
	 * This method discovers feed generators from known service DIDs, fetches their records,
	 * and invokes the provided callback for each discovered feed.
	 * 
	 * @param options Discovery and processing options
	 * @returns Statistics about the backfill operation
	 */
	async backfillFeedGenerators(options: FeedGeneratorDiscoveryOptions = {}): Promise<{
		discovered: number;
		fetched: number;
		skipped: number;
		errors: number;
	}> {
		const batchSize = options.batchSize || 100;
		const stats = {
			discovered: 0,
			fetched: 0,
			skipped: 0,
			errors: 0,
		};

		// Discover all feed generator URIs
		const feedGenUris = await this.discoverAllFeedGenerators(options);
		stats.discovered = feedGenUris.length;

		if (feedGenUris.length === 0) {
			console.log("[FeedGen Fetcher] No feed generators discovered");
			return stats;
		}

		console.log(`[FeedGen Fetcher] Fetching and processing ${feedGenUris.length} feed generators...`);

		// Process in batches
		for (let i = 0; i < feedGenUris.length; i += batchSize) {
			const batch = feedGenUris.slice(i, i + batchSize);
			
			for (const uri of batch) {
				try {
					// Fetch from Slingshot
					const record = await this.fetchFeedGenerator(uri);
					
					if (record) {
						stats.fetched++;
						
						// Invoke callback if provided
						if (options.onFeedDiscovered) {
							await options.onFeedDiscovered(record);
						}
					} else {
						stats.skipped++;
					}
				} catch (err) {
					stats.errors++;
					console.error(`[FeedGen Fetcher] Error processing ${uri}:`, err);
				}
				
				// Rate limiting
				await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY));
			}

			// Progress callback
			if (options.onProgress) {
				const processed = Math.min(i + batchSize, feedGenUris.length);
				options.onProgress(processed, feedGenUris.length);
			}
		}

		console.log(`[FeedGen Fetcher] Backfill complete: ${stats.fetched} fetched, ${stats.skipped} skipped, ${stats.errors} errors`);
		return stats;
	}
}
