/// The HTTP interface, shared by the Worker of the engine and the gateway. It does not import the engine, so that
/// the gateway can use it without the module of the engine in its bundle.

/// The answer to a query, as plain data, so that it can come back from an `EngineHost` over RPC.
export type Answer =
	| { ok: true; body: Uint8Array; timing: string }
	| { ok: false; code: number; message: string; timing: string };

/// A query as the HTTP interface gets it, for an engine. The body is a stream, which can go over RPC without a limit of
/// size, and which the engine reads as it runs the query.
export interface QueryRequest {
	/// The `query` parameter. Without it, the body is the query.
	query: string | null;
	/// The body of a `POST`: the data of an `INSERT` after `query`, or the query.
	body: ReadableStream<Uint8Array> | null;
	/// The `Content-Encoding` of the body: `gzip`, `deflate`, `br`, `zstd`, `lz4`, `xz`, `bz2`, or empty.
	encoding: string;
	/// `default_format`.
	format: string;
	readonly: boolean;
}

/// The code of the exception of a query that failed in the platform rather than in the engine, for example because
/// the Durable Object of its engine was reset by a deployment: `UNKNOWN_EXCEPTION`. Such a query can be sent again.
const PLATFORM_FAILURE = 1002;

/// How long a request waits for its query, after which it is answered with status 504 and the code
/// `TIMEOUT_EXCEEDED`. The query may still finish, and its changes may still be committed.
const ANSWER_TIMEOUT_MS = 10 * 60 * 1000;
const TIMEOUT_EXCEEDED = 159;

/// The secrets that authorize queries.
export interface Tokens {
	/// Full access, as `Authorization: Bearer <token>`.
	ENGINE_TOKEN: string;
	/// Queries that may only read, which run with the setting `readonly` = 2: no writes and no DDL, but their settings
	/// can change. Optional.
	ENGINE_READONLY_TOKEN?: string;
}

/// A subset of the HTTP interface of ClickHouse:
/// - `GET /ping` answers `Ok.`;
/// - the query is the `query` parameter, or the body of a `POST`; with both, the body follows the query as the
///   data of an `INSERT`, and streams, whatever its size;
/// - the body may be compressed, as its `Content-Encoding` says;
/// - `default_format` is the output format of a query without a `FORMAT` clause;
/// - the queries with the same `session_id` run in the same engine, so that they see the same tables of `Memory`;
/// - an exception is returned with status 500, its code in `X-ClickHouse-Exception-Code`; a failure of the platform
///   that the query can be sent again after is returned with status 503, and a query that takes more than ten
///   minutes with status 504;
/// - every request except `/ping` needs `Authorization: Bearer <ENGINE_TOKEN>`, or `<ENGINE_READONLY_TOKEN>` for
///   queries that may only read.
export async function handleRequest(
	request: Request,
	env: Tokens,
	answer: (request: QueryRequest, session: string | null) => Promise<Answer>,
): Promise<Response> {
	const url = new URL(request.url);
	if (url.pathname === "/ping") {
		return new Response("Ok.\n");
	}
	const access = await authorize(request, env);
	if (access === undefined) {
		return new Response("Unauthorized\n", { status: 401, headers: { "WWW-Authenticate": "Bearer" } });
	}
	if (url.pathname !== "/") {
		return new Response("Not found\n", { status: 404 });
	}

	const query = url.searchParams.get("query");
	const body = request.method === "POST" ? request.body : null;
	if (query === null && body === null) {
		return new Response("Empty query\n", { status: 400 });
	}

	const query_request: QueryRequest = {
		query,
		body,
		encoding: request.headers.get("Content-Encoding") ?? "",
		format: url.searchParams.get("default_format") ?? "",
		readonly: access === "read",
	};
	let result: Answer | "timeout";
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const timeout = new Promise<"timeout">((resolve) => (timer = setTimeout(() => resolve("timeout"), ANSWER_TIMEOUT_MS)));
		result = await Promise.race([answer(query_request, url.searchParams.get("session_id")), timeout]);
	} catch (error) {
		return new Response(`Code: ${PLATFORM_FAILURE}. The engine failed, the query can be sent again: ${String(error)}\n`, {
			status: 503,
			headers: { "X-ClickHouse-Exception-Code": String(PLATFORM_FAILURE), "Retry-After": "1" },
		});
	} finally {
		if (timer !== undefined) {
			clearTimeout(timer);
		}
	}
	if (result === "timeout") {
		const message = `Code: ${TIMEOUT_EXCEEDED}. The query took more than ${ANSWER_TIMEOUT_MS / 1000} s; it may still finish, and its changes may still be committed.`;
		return new Response(`${message}\n`, { status: 504, headers: { "X-ClickHouse-Exception-Code": String(TIMEOUT_EXCEEDED) } });
	}
	if (!result.ok) {
		return new Response(result.message + "\n", {
			status: 500,
			headers: { "X-ClickHouse-Exception-Code": String(result.code), "Server-Timing": result.timing },
		});
	}
	return new Response(result.body, { headers: { "Server-Timing": result.timing } });
}

/// Which token the request presents: `ENGINE_TOKEN` for full access, `ENGINE_READONLY_TOKEN` for reading. The
/// tokens are compared in constant time, and a token that is not set authorizes nothing.
async function authorize(request: Request, env: Tokens): Promise<"write" | "read" | undefined> {
	const header = request.headers.get("Authorization") ?? "";
	const prefix = "Bearer ";
	if (!header.startsWith(prefix)) {
		return undefined;
	}
	const given = await digest(header.slice(prefix.length));
	if (env.ENGINE_TOKEN && crypto.subtle.timingSafeEqual(given, await digest(env.ENGINE_TOKEN))) {
		return "write";
	}
	if (env.ENGINE_READONLY_TOKEN && crypto.subtle.timingSafeEqual(given, await digest(env.ENGINE_READONLY_TOKEN))) {
		return "read";
	}
	return undefined;
}

/// `timingSafeEqual` needs inputs of the same length, so it compares digests.
function digest(token: string): Promise<ArrayBuffer> {
	return crypto.subtle.digest("SHA-256", new TextEncoder().encode(token));
}

