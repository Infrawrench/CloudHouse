import type { Coordination } from "./coordination";

export interface Env {
	COORDINATION: DurableObjectNamespace<Coordination>;
}
