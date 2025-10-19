/**
 * Feed Generator Backfill Example
 * 
 * This example shows how to use the FeedGeneratorIndexer to discover and index
 * feed generators using the Constellation + Slingshot approach.
 * 
 * Usage:
 *   deno run --allow-net --allow-env examples/feedgen-backfill.ts
 */

import { createDb } from "@zeppelin-social/bsky/dist/data-plane/server/db/index.js";
import { FeedGeneratorIndexer } from "../src/feedgen-indexer.ts";

// Database configuration from environment
const DB_URL = Deno.env.get("BSKY_DB_POSTGRES_URL") || 
	`postgres://${Deno.env.get("POSTGRES_USER") || "pg"}:${Deno.env.get("POSTGRES_PASSWORD")}@${Deno.env.get("POSTGRES_HOST") || "localhost"}:5432/bsky`;

async function main() {
	console.log("=".repeat(60));
	console.log("Feed Generator Backfill Example");
	console.log("=".repeat(60));
	console.log();

	// Create database connection
	console.log("Connecting to database...");
	const db = createDb({
		url: DB_URL,
		schema: "bsky",
	});

	// Create indexer instance
	const indexer = new FeedGeneratorIndexer({
		db,
		verbose: true,
		batchSize: 100,
	});

	try {
		// Run one-time backfill
		console.log("\nRunning one-time backfill...\n");
		const stats = await indexer.runBackfill();

		console.log("\n" + "=".repeat(60));
		console.log("Backfill Complete!");
		console.log("=".repeat(60));
		console.log(`Discovered: ${stats.discovered}`);
		console.log(`Indexed:    ${stats.indexed}`);
		console.log(`Skipped:    ${stats.skipped}`);
		console.log(`Errors:     ${stats.errors}`);
		console.log();

	} catch (err) {
		console.error("Fatal error:", err);
		Deno.exit(1);
	} finally {
		// Clean up
		await db.close();
	}
}

// Run if this is the main module
if (import.meta.main) {
	main();
}
