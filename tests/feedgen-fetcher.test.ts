/**
 * Feed Generator Fetcher Test
 * 
 * Simple test to verify the Constellation + Slingshot integration works.
 * This doesn't require database access - just tests the API integrations.
 * 
 * Usage:
 *   deno run --allow-net tests/feedgen-fetcher.test.ts
 */

import { FeedGeneratorFetcher } from "../src/feedgen-fetcher.ts";

async function testDiscovery() {
	console.log("\n=== Test 1: Discovery from single service ===");
	const fetcher = new FeedGeneratorFetcher();
	
	// Test with Bluesky's discover service
	const serviceDid = "did:web:discover.bsky.app";
	console.log(`Discovering feeds from ${serviceDid}...`);
	
	const uris = await fetcher.discoverFeedGeneratorsByService(serviceDid);
	console.log(`✓ Found ${uris.length} feed URIs`);
	
	if (uris.length > 0) {
		console.log(`  Example: ${uris[0]}`);
		return true;
	} else {
		console.log(`  ⚠ Warning: No feeds found (API may have changed)`);
		return false;
	}
}

async function testFetching() {
	console.log("\n=== Test 2: Fetch specific feed ===");
	const fetcher = new FeedGeneratorFetcher();
	
	// Known feed URI (Bluesky's "What's Hot")
	const uri = "at://did:plc:z72i7hdynmk6r22z27h6tvur/app.bsky.feed.generator/whats-hot";
	console.log(`Fetching: ${uri}...`);
	
	const record = await fetcher.fetchFeedGenerator(uri);
	
	if (record) {
		console.log(`✓ Successfully fetched feed`);
		console.log(`  Name: ${record.value.displayName}`);
		console.log(`  Description: ${record.value.description || "(none)"}`);
		console.log(`  Service DID: ${record.value.did}`);
		console.log(`  CID: ${record.cid}`);
		return true;
	} else {
		console.log(`✗ Failed to fetch feed`);
		return false;
	}
}

async function testDiscoverAll() {
	console.log("\n=== Test 3: Discover from all known services ===");
	const fetcher = new FeedGeneratorFetcher();
	
	console.log(`Discovering feeds from all known services...`);
	const uris = await fetcher.discoverAllFeedGenerators();
	
	console.log(`✓ Found ${uris.length} unique feed URIs across all services`);
	
	if (uris.length > 0) {
		console.log(`  Sample URIs:`);
		for (let i = 0; i < Math.min(3, uris.length); i++) {
			console.log(`    - ${uris[i]}`);
		}
		return true;
	} else {
		console.log(`  ⚠ Warning: No feeds found`);
		return false;
	}
}

async function testBackfillWithCallbacks() {
	console.log("\n=== Test 4: Backfill with callbacks (limited) ===");
	const fetcher = new FeedGeneratorFetcher();
	
	const discovered: string[] = [];
	let progressCalls = 0;
	
	// Limit to just one service and small batch for testing
	const stats = await fetcher.backfillFeedGenerators({
		serviceDids: ["did:web:discover.bsky.app"],
		batchSize: 5, // Small batch for testing
		onFeedDiscovered: async (record) => {
			discovered.push(record.uri);
			if (discovered.length <= 3) {
				console.log(`  Discovered: ${record.value.displayName}`);
			}
		},
		onProgress: (processed, total) => {
			progressCalls++;
		},
	});
	
	console.log(`✓ Backfill completed`);
	console.log(`  Total discovered: ${stats.discovered}`);
	console.log(`  Successfully fetched: ${stats.fetched}`);
	console.log(`  Skipped: ${stats.skipped}`);
	console.log(`  Errors: ${stats.errors}`);
	console.log(`  Progress callbacks: ${progressCalls}`);
	
	return stats.discovered > 0;
}

async function main() {
	console.log("╔════════════════════════════════════════════════════════╗");
	console.log("║  Feed Generator Fetcher Integration Tests             ║");
	console.log("╚════════════════════════════════════════════════════════╝");
	
	const results = {
		discovery: false,
		fetching: false,
		discoverAll: false,
		backfill: false,
	};
	
	try {
		results.discovery = await testDiscovery();
		results.fetching = await testFetching();
		results.discoverAll = await testDiscoverAll();
		results.backfill = await testBackfillWithCallbacks();
		
		console.log("\n╔════════════════════════════════════════════════════════╗");
		console.log("║  Test Results                                          ║");
		console.log("╚════════════════════════════════════════════════════════╝");
		console.log(`  Discovery from single service: ${results.discovery ? "✓ PASS" : "✗ FAIL"}`);
		console.log(`  Fetch specific feed:           ${results.fetching ? "✓ PASS" : "✗ FAIL"}`);
		console.log(`  Discover from all services:    ${results.discoverAll ? "✓ PASS" : "✗ FAIL"}`);
		console.log(`  Backfill with callbacks:       ${results.backfill ? "✓ PASS" : "✗ FAIL"}`);
		
		const allPassed = Object.values(results).every(r => r);
		
		if (allPassed) {
			console.log("\n✓ All tests passed!");
			Deno.exit(0);
		} else {
			console.log("\n⚠ Some tests failed - check output above");
			Deno.exit(1);
		}
		
	} catch (err) {
		console.error("\n✗ Fatal error during tests:", err);
		Deno.exit(1);
	}
}

if (import.meta.main) {
	main();
}
