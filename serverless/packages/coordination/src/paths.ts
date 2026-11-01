/// Node paths are absolute, slash-separated, with no empty, `.` or `..` components, and no
/// trailing slash except for the root itself.
export function isValidPath(path: string): boolean {
	if (path === "/") {
		return true;
	}
	if (!path.startsWith("/") || path.endsWith("/") || path.includes("\0")) {
		return false;
	}
	return path
		.slice(1)
		.split("/")
		.every((part) => part !== "" && part !== "." && part !== "..");
}

export function parentPath(path: string): string {
	const pos = path.lastIndexOf("/");
	return pos === 0 ? "/" : path.slice(0, pos);
}

export function baseName(path: string): string {
	return path.slice(path.lastIndexOf("/") + 1);
}

/// The suffix of a sequential node, as in ZooKeeper: ten digits, zero-padded.
export function sequentialSuffix(seq: number): string {
	return seq.toString().padStart(10, "0");
}
