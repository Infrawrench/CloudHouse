import type { Env as EngineEnv } from "../src/env";

declare global {
	namespace Cloudflare {
		interface Env extends EngineEnv {}
	}
}
