/// The HTTP interface, shared by the Worker of the engine and the gateway. It does not import the engine, so that
/// the gateway can use it without the module of the engine in its bundle.

/// The answer to a query, as plain data, so that it can come back from an `EngineHost` over RPC.
export type Answer =
	| { ok: true; body: Uint8Array; timing: string }
	| { ok: false; code: number; message: string; timing: string };

/// The secrets that authorize queries.
export interface Tokens {
	/// Full access, as `Authorization: Bearer <token>`.
	ENGINE_TOKEN: string;
	/// Queries that may only read, which run with the setting `readonly` = 1. Optional.
	ENGINE_READONLY_TOKEN?: string;
}

/// A subset of the HTTP interface of ClickHouse:
/// - `GET /ping` answers `Ok.`;
/// - the query is the `query` parameter, or the body of a `POST`; with both, the body follows the query as the
///   data of an `INSERT`;
/// - `default_format` is the output format of a query without a `FORMAT` clause;
/// - the queries with the same `session_id` run in the same engine, so that they see the same tables of `Memory`;
/// - an exception is returned with status 500, its code in `X-ClickHouse-Exception-Code`;
/// - every request except `/ping` needs `Authorization: Bearer <ENGINE_TOKEN>`, or `<ENGINE_READONLY_TOKEN>` for
///   queries that may only read.
export async function handleRequest(
	request: Request,
	env: Tokens,
	answer: (text: Uint8Array, format: string, readonly: boolean, session: string | null) => Promise<Answer>,
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
	const body = request.method === "POST" ? new Uint8Array(await request.arrayBuffer()) : new Uint8Array();
	if (query === null && body.length === 0) {
		return new Response("Empty query\n", { status: 400 });
	}

	const text = query === null ? body : concat(new TextEncoder().encode(query + "\n"), body);
	const format = url.searchParams.get("default_format") ?? "";
	const result = await answer(text, format, access === "read", url.searchParams.get("session_id"));
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

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
	const result = new Uint8Array(a.length + b.length);
	result.set(a);
	result.set(b, a.length);
	return result;
}
