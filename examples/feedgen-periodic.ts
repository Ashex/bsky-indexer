/**
 * Feed Generator Periodic Backfill Example
 * 
 * This example shows how to run periodic feed generator backfills alongside
 * the main firehose subscription. This keeps your feed generator index up-to-date
 * with new feeds as they're created.
 * 
 * Usage:
 *   deno run --allow-net --allow-env examples/feedgen-periodic.ts
 */

import { createDb } from "@zeppelin-social/bsky/dist/data-plane/server/db/index.js";
import { FeedGeneratorIndexer } from "../src/feedgen-indexer.ts";

// Database configuration from environment
const DB_URL = Deno.env.get("BSKY_DB_POSTGRES_URL") || 
	`postgres://${Deno.env.get("POSTGRES_USER") || "pg"}:${Deno.env.get("POSTGRES_PASSWORD")}@${Deno.env.get("POSTGRES_HOST") || "localhost"}:5432/bsky`;

// Backfill interval (default: 6 hours)
const BACKFILL_INTERVAL_MS = parseInt(Deno.env.get("FEEDGEN_BACKFILL_INTERVAL") || "") || 
	1000 * 60 * 60 * 6;

async function main() {
	console.log("=".repeat(60));
	console.log("Feed Generator Periodic Backfill");
	console.log("=".repeat(60));
	console.log(`Backfill interval: ${BACKFILL_INTERVAL_MS / 1000 / 60} minutes`);
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
		verbose: false, // Less verbose for periodic runs
		batchSize: 100,
	});

	try {
		// Start periodic backfill
		console.log("Starting periodic feed generator backfill...");
		const timerId = indexer.startPeriodicBackfill(BACKFILL_INTERVAL_MS);

		console.log("Periodic backfill started. Press Ctrl+C to stop.");
		console.log();

		// Keep the process running
		// In a real application, you'd integrate this with your main service
		await new Promise(() => {}); // Never resolves

	} catch (err) {
		console.error("Fatal error:", err);
		Deno.exit(1);
	}
}

// Handle graceful shutdown
Deno.addSignalListener("SIGINT", () => {
	console.log("\nReceived SIGINT, shutting down...");
	Deno.exit(0);
});

Deno.addSignalListener("SIGTERM", () => {
	console.log("\nReceived SIGTERM, shutting down...");
	Deno.exit(0);
});

// Run if this is the main module
if (import.meta.main) {
	main();
}
