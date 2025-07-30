import { IndexingService } from "@zeppelin-social/bsky/dist/data-plane/server/indexing/index.js";
import { RecordProcessor } from "@zeppelin-social/bsky/dist/data-plane/server/indexing/processor.js";
import type { Database } from "@zeppelin-social/bsky";
import type { AtUri } from "@atproto/syntax";
import { stringifyLex } from "@atproto/lexicon";
import { WriteOpAction } from "@atproto/repo";
import type { CID } from "multiformats/cid";

const DEFAULT_TIMESTAMP_DELTA = 1000 * 60 * 10; // 10 minutes

export class CustomIndexingService extends IndexingService {
	protected maxTsDelta: number;

	constructor(
		...[db, idResolver, background, maxTsDelta = DEFAULT_TIMESTAMP_DELTA]: [
			...ConstructorParameters<typeof IndexingService>,
			number?,
		]
	) {
		super(db, idResolver, background);
		this.maxTsDelta = maxTsDelta;
		this.records = Object.fromEntries(
			Object.entries(this.records).map((
				[key, value],
			) => [key, CustomRecordProcessor.fromProcessor(value)]),
		) as {
			[K in keyof typeof this.records]: (typeof this.records)[K] extends
				RecordProcessor<infer S, infer T> ? CustomRecordProcessor<S, T> : never;
		};
	}

	override transact(txn: Database) {
		txn.assertTransaction();
		return new CustomIndexingService(txn, this.idResolver, this.background);
	}

	override async indexRecord(
		uri: AtUri,
		cid: CID,
		obj: unknown,
		action: WriteOpAction.Create | WriteOpAction.Update,
		timestamp: string,
		opts?: { disableNotifs?: boolean; disableLabels?: boolean },
	) {
		if (!this.findIndexerForCollection(uri.collection)) return;

		const timeMs = new Date(timestamp).getTime();
		const boundedTimestamp = !isNaN(timeMs) && Math.abs(timeMs - Date.now()) <= this.maxTsDelta
			? timestamp // if the event time is within delta of current time, use it as indexedAt
			: new Date().toISOString(); // otherwise, decide it ourselves

		this.db.assertNotTransaction();
		await this.db.transaction(async (txn) => {
			const indexingTx = this.transact(txn);
			const indexer = indexingTx.findIndexerForCollection(uri.collection)!;

			if (action === WriteOpAction.Create) {
				await indexer.insertRecord(uri, cid, obj, boundedTimestamp, opts);
			} else {
				await indexer.updateRecord(uri, cid, obj, boundedTimestamp);
			}
		});
	}

	override async deleteRecord(uri: AtUri, cascading = false) {
		if (!this.findIndexerForCollection(uri.collection)) return;
		this.db.assertNotTransaction();
		await this.db.transaction(async (txn) => {
			const indexingTx = this.transact(txn);
			const indexer = indexingTx.findIndexerForCollection(uri.collection)!;
			await indexer.deleteRecord(uri, cascading);
		});
	}
}

export class CustomRecordProcessor<S, T> extends RecordProcessor<S, T> {
	static fromProcessor<S, T>(processor: RecordProcessor<S, T>): CustomRecordProcessor<S, T> {
		// disk doesn't like lots of concurrent attempts to lock *_agg
		// @ts-expect-error — private property
		processor.background.queue.concurrency = 15;
		// @ts-expect-error — private properties
		return new CustomRecordProcessor(processor.appDb, processor.background, processor.params);
	}

	override async insertRecord(
		uri: AtUri,
		cid: CID,
		obj: unknown,
		timestamp: string,
		opts?: { disableNotifs?: boolean },
	): Promise<void> {
		// @ts-expect-error — private property
		const params = this.params;
		this.assertValidRecord(obj);
		const [inserted] = await Promise.allSettled([
			params.insertFn(
				this.db,
				uri,
				cid,
				obj,
				timestamp,
			),
			this.db
				.insertInto("record")
				.values({
					uri: uri.toString(),
					cid: cid.toString(),
					did: uri.host,
					json: stringifyLex(obj),
					indexedAt: timestamp,
				})
				.onConflict((oc) => oc.doNothing())
				.execute(),
		]);

		if (inserted.status === "fulfilled" && inserted.value) {
			this.aggregateOnCommit(inserted.value);
			if (!opts?.disableNotifs) {
				await this.handleNotifs({ inserted: inserted.value });
			}
			return;
		}

		// if duplicate, insert into duplicates table with no events
		// @ts-expect-error — private property
		this.background.add(async () => {
			const found = await params.findDuplicate(this.db, uri, obj);
			if (found && found.toString() !== uri.toString()) {
				await this.db
					.insertInto("duplicate_record")
					.values({
						uri: uri.toString(),
						cid: cid.toString(),
						duplicateOf: found.toString(),
						indexedAt: timestamp,
					})
					.onConflict((oc) => oc.doNothing())
					.execute();
			}
		});
	}

	override aggregateOnCommit(indexed: T) {
		// @ts-expect-error — private property
		const { updateAggregates } = this.params;
		if (!updateAggregates) return;
		// @ts-expect-error — private property
		this.appDb.onCommit(() => {
			// @ts-expect-error — private property
			this.background.add((db) => updateAggregates(db.db, indexed));
		});
	}
}
