// SOT: environment-registry, env-colours, read-only-default
import type { Environment } from "./bindings";
import { keysOf } from "./records";

// WHAT:  Badge colour tokens and defaults per environment.
// WHERE: src/styles/globals.css (--color-env-*), src-tauri/src/model/connection.rs
export interface EnvironmentMeta {
  label: string;
  dot: string;
  text: string;
  stripe: string;
  readOnlyDefault: boolean;
}

export const ENVIRONMENTS = {
  none: { label: "None", dot: "bg-muted", text: "text-muted", stripe: "bg-border", readOnlyDefault: false },
  local: { label: "Local", dot: "bg-env-local", text: "text-env-local", stripe: "bg-env-local", readOnlyDefault: false },
  staging: { label: "Staging", dot: "bg-env-staging", text: "text-env-staging", stripe: "bg-env-staging", readOnlyDefault: false },
  production: { label: "Production", dot: "bg-env-production", text: "text-env-production", stripe: "bg-env-production", readOnlyDefault: true },
} satisfies Record<Environment, EnvironmentMeta>;

export const ENVIRONMENT_ORDER: Environment[] = keysOf(ENVIRONMENTS);

export function environmentMeta(env: Environment): EnvironmentMeta {
  return ENVIRONMENTS[env];
}
