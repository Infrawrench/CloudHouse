/// Parts as the catalog lists them: the engine names a part `<uuid>_<rows>_<bytes>_<partition id>`, with the size of
/// its data and its partition.

export interface Part {
	name: string;
	rows: number;
	bytes: number;
	partition: string;
}

const NAME = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_(\d+)_(\d+)_(.+)$/;

export function parsePart(name: string): Part {
	const match = NAME.exec(name);
	if (match === null) {
		throw new Error(`Not the name of a part: ${name}`);
	}
	return { name, rows: Number(match[1]), bytes: Number(match[2]), partition: match[3]! };
}

/// The parts of each partition.
export function byPartition(names: readonly string[]): string[][] {
	const groups = new Map<string, string[]>();
	for (const name of names) {
		const partition = parsePart(name).partition;
		groups.set(partition, [...(groups.get(partition) ?? []), name]);
	}
	return [...groups.values()];
}

export function partKey(name: string): string {
	return `parts/${name}`;
}

export function totalBytes(names: readonly string[]): number {
	return names.reduce((sum, name) => sum + parsePart(name).bytes, 0);
}

/// The parts to merge next: the smallest ones of a partition, as many as fit in `max_bytes` and `max_parts`, if at
/// least two do. Merging the small parts first keeps the number of parts down at the least cost, as `MergeTree` does.
/// Of the partitions, the one with the most parts to merge goes first.
export function chooseMerge(names: readonly string[], max_bytes: number, max_parts: number): string[] | undefined {
	let best: string[] | undefined;
	for (const partition of byPartition(names)) {
		const chosen = chooseMergeInPartition(partition, max_bytes, max_parts);
		if (chosen !== undefined && (best === undefined || chosen.length > best.length)) {
			best = chosen;
		}
	}
	return best;
}

function chooseMergeInPartition(names: readonly string[], max_bytes: number, max_parts: number): string[] | undefined {
	const parts = names.map(parsePart).sort((a, b) => a.bytes - b.bytes);
	const chosen: string[] = [];
	let bytes = 0;
	for (const part of parts) {
		if (chosen.length === max_parts || bytes + part.bytes > max_bytes) {
			break;
		}
		chosen.push(part.name);
		bytes += part.bytes;
	}
	return chosen.length >= 2 ? chosen : undefined;
}

/// Splits the parts of each partition into groups of at most `max_bytes`, for the merges that apply a TTL to every
/// part. A part that is larger is a group of its own.
export function groupParts(names: readonly string[], max_bytes: number): string[][] {
	return byPartition(names).flatMap((partition) => groupPartsInPartition(partition, max_bytes));
}

function groupPartsInPartition(names: readonly string[], max_bytes: number): string[][] {
	const groups: string[][] = [];
	let current: string[] = [];
	let bytes = 0;
	for (const part of names.map(parsePart).sort((a, b) => a.bytes - b.bytes)) {
		if (current.length > 0 && bytes + part.bytes > max_bytes) {
			groups.push(current);
			current = [];
			bytes = 0;
		}
		current.push(part.name);
		bytes += part.bytes;
	}
	if (current.length > 0) {
		groups.push(current);
	}
	return groups;
}

/// Splits the parts among shards of about `shard_bytes` each, at most `max_shards`, the largest parts first into the
/// least loaded shard.
export function shardParts(names: readonly string[], shard_bytes: number, max_shards: number): string[][] {
	const parts = names.map(parsePart).sort((a, b) => b.bytes - a.bytes);
	const total = parts.reduce((sum, part) => sum + part.bytes, 0);
	const count = Math.max(1, Math.min(max_shards, parts.length, Math.ceil(total / shard_bytes)));
	const shards = Array.from({ length: count }, () => ({ bytes: 0, parts: [] as string[] }));
	for (const part of parts) {
		const shard = shards.reduce((least, candidate) => (candidate.bytes < least.bytes ? candidate : least));
		shard.parts.push(part.name);
		shard.bytes += part.bytes;
	}
	return shards.map((shard) => shard.parts);
}
