import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
	plugins: [
		cloudflareTest({
			wrangler: { configPath: "./wrangler.jsonc" },
			miniflare: { bindings: { ENGINE_TOKEN: "test-token", ENGINE_READONLY_TOKEN: "read-token", ENGINE_POOL: "1", KEEP_WARM_SECONDS: "50" } },
		}),
	],
});
