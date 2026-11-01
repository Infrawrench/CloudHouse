import type { Env as CoordinationEnv } from "../src/env";

declare global {
	namespace Cloudflare {
		interface Env extends CoordinationEnv {}
	}
}
