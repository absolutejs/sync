import type { RowKey } from './types';
import type { SearchHit, SearchIndex } from './search';

/**
 * An incremental full-text index with BM25 ranking — the keyword-search half of
 * the search surface (see {@link createVectorIndex} for semantic search). Pure
 * and dependency-free: an in-memory inverted index maintained as rows are
 * added/removed, so a {@link defineSearchCollection} stays live as the corpus
 * changes. For a large corpus back it with your DB's FTS instead; this is the
 * BYO, no-extension default.
 */

export type TextIndexOptions<T> = {
	/** Row identity. */
	key: (row: T) => RowKey;
	/** Fields whose text is indexed. Their values are stringified and joined. */
	fields: (keyof T)[];
	/**
	 * Split text into terms. Defaults to lowercase alphanumeric runs. Provide your
	 * own for stemming, n-grams, or a different alphabet — used for both indexing
	 * and querying, so the two always agree.
	 */
	tokenize?: (text: string) => string[];
	/** Terms to drop (e.g. `the`, `a`). Applied after `tokenize`. */
	stopwords?: Iterable<string>;
	/** BM25 term-frequency saturation. Defaults to 1.5. */
	k1?: number;
	/** BM25 length normalization (0–1). Defaults to 0.75. */
	b?: number;
};

const defaultTokenize = (text: string): string[] =>
	text.toLowerCase().match(/[a-z0-9]+/g) ?? [];

type Doc<T> = { row: T; len: number; tf: Map<string, number> };

/**
 * Build an incremental BM25 full-text index over rows of `T`. Implements the
 * {@link SearchIndex} interface, so it plugs straight into a search collection.
 */
export const createTextIndex = <T>(
	options: TextIndexOptions<T>
): SearchIndex<T, string> => {
	const { key, fields } = options;
	const tokenize = options.tokenize ?? defaultTokenize;
	const stopwords = new Set(options.stopwords ?? []);
	const k1 = options.k1 ?? 1.5;
	const b = options.b ?? 0.75;

	const docs = new Map<RowKey, Doc<T>>();
	// term -> set of doc keys containing it (postings). `df` is its size.
	const postings = new Map<string, Set<RowKey>>();
	let totalLen = 0;

	const termsOf = (row: T): string[] => {
		const text = fields
			.map((field) => {
				const value = row[field];
				return value === undefined || value === null
					? ''
					: String(value);
			})
			.join(' ');
		return tokenize(text).filter((term) => !stopwords.has(term));
	};

	const remove = (rowKey: RowKey) => {
		const doc = docs.get(rowKey);
		if (doc === undefined) {
			return;
		}
		for (const term of doc.tf.keys()) {
			const set = postings.get(term);
			if (set !== undefined) {
				set.delete(rowKey);
				if (set.size === 0) {
					postings.delete(term);
				}
			}
		}
		totalLen -= doc.len;
		docs.delete(rowKey);
	};

	const add = (row: T) => {
		const rowKey = key(row);
		// Upsert: replace any prior version of this row.
		remove(rowKey);
		const terms = termsOf(row);
		const tf = new Map<string, number>();
		for (const term of terms) {
			tf.set(term, (tf.get(term) ?? 0) + 1);
		}
		for (const term of tf.keys()) {
			let set = postings.get(term);
			if (set === undefined) {
				set = new Set();
				postings.set(term, set);
			}
			set.add(rowKey);
		}
		docs.set(rowKey, { row, len: terms.length, tf });
		totalLen += terms.length;
	};

	const search = (query: string, limit: number): SearchHit<T>[] => {
		const total = docs.size;
		if (total === 0) {
			return [];
		}
		const avgdl = totalLen / total;
		const queryTerms = new Set(
			tokenize(query).filter((term) => !stopwords.has(term))
		);
		const scores = new Map<RowKey, number>();
		for (const term of queryTerms) {
			const set = postings.get(term);
			if (set === undefined) {
				continue;
			}
			const df = set.size;
			// BM25+ idf form — always positive, even for common terms.
			const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
			for (const rowKey of set) {
				const doc = docs.get(rowKey)!;
				const freq = doc.tf.get(term) ?? 0;
				const norm =
					(freq * (k1 + 1)) /
					(freq + k1 * (1 - b + (b * doc.len) / avgdl));
				scores.set(rowKey, (scores.get(rowKey) ?? 0) + idf * norm);
			}
		}
		return [...scores.entries()]
			.map(([rowKey, score]) => ({ row: docs.get(rowKey)!.row, score }))
			.sort((first, second) => second.score - first.score)
			.slice(0, limit);
	};

	return {
		add,
		remove,
		search,
		size: () => docs.size,
		clear: () => {
			docs.clear();
			postings.clear();
			totalLen = 0;
		}
	};
};
