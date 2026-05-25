import type { RowKey } from './types';
import type { SearchHit, SearchIndex } from './search';

/**
 * An incremental vector index for semantic / similarity search — the embeddings
 * half of the search surface (see {@link createTextIndex} for keyword search).
 * Pure and dependency-free: an exact (brute-force) k-NN over in-memory vectors,
 * maintained as rows are added/removed, so a {@link defineSearchCollection}
 * stays live. Pairs naturally with `@absolutejs/ai` / `@absolutejs/rag` for RAG
 * retrieval on your own data. Exact search is O(n·d) per query — fine for tens
 * of thousands of vectors; for more, back it with pgvector and a real ANN index.
 */

/** Similarity metric. `cosine`/`dot` rank higher = closer; `euclidean` too (negated distance). */
export type VectorMetric = 'cosine' | 'dot' | 'euclidean';

export type VectorIndexOptions<T> = {
	/** Row identity. */
	key: (row: T) => RowKey;
	/** Extract a row's embedding vector. */
	embedding: (row: T) => number[];
	/** Similarity metric. Defaults to `cosine`. */
	metric?: VectorMetric;
};

type Entry<T> = { row: T; vec: number[]; norm: number };

const dot = (first: number[], second: number[]): number => {
	const length = Math.min(first.length, second.length);
	let sum = 0;
	for (let index = 0; index < length; index += 1) {
		sum += first[index]! * second[index]!;
	}
	return sum;
};

const normOf = (vec: number[]): number => Math.sqrt(dot(vec, vec));

const euclidean = (first: number[], second: number[]): number => {
	const length = Math.max(first.length, second.length);
	let sum = 0;
	for (let index = 0; index < length; index += 1) {
		const delta = (first[index] ?? 0) - (second[index] ?? 0);
		sum += delta * delta;
	}
	return Math.sqrt(sum);
};

/**
 * Build an incremental vector index over rows of `T`. Implements the
 * {@link SearchIndex} interface (queried by a query vector), so it plugs
 * straight into a search collection.
 */
export const createVectorIndex = <T>(
	options: VectorIndexOptions<T>
): SearchIndex<T, number[]> => {
	const { key, embedding } = options;
	const metric = options.metric ?? 'cosine';
	const entries = new Map<RowKey, Entry<T>>();

	const score = (
		query: number[],
		queryNorm: number,
		entry: Entry<T>
	): number => {
		if (metric === 'dot') {
			return dot(query, entry.vec);
		}
		if (metric === 'euclidean') {
			return -euclidean(query, entry.vec);
		}
		// cosine: dot / (|q|·|v|); a zero vector has no direction → score 0.
		const denominator = queryNorm * entry.norm;
		return denominator === 0 ? 0 : dot(query, entry.vec) / denominator;
	};

	return {
		add: (row) => {
			const vec = embedding(row);
			entries.set(key(row), { row, vec, norm: normOf(vec) });
		},
		remove: (rowKey) => {
			entries.delete(rowKey);
		},
		search: (query, limit): SearchHit<T>[] => {
			const queryNorm = normOf(query);
			return [...entries.values()]
				.map((entry) => ({
					row: entry.row,
					score: score(query, queryNorm, entry)
				}))
				.sort((first, second) => second.score - first.score)
				.slice(0, limit);
		},
		size: () => entries.size,
		clear: () => {
			entries.clear();
		}
	};
};
