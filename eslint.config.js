// SOT: lint-rules, ts-escape-hatch-ban, ipc-boundary-rule
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import globals from "globals";

// WHAT:  ESLint flat config enforcing the guardrail on the TypeScript side.
// WHY:   Instructions lose to patterns; lint errors do not. Every rule that
//        matters exists here as an error, not only as a sentence in CLAUDE.md.
// HOW:   `pnpm lint` runs this; `pnpm check` chains it with tsc and cargo.
// WHERE: scripts/guardrail.py (structural checks lint cannot express)
export default tseslint.config(
  { ignores: ["dist", "src-tauri", "node_modules", "src/lib/bindings/**"] },
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked,
  {
    files: ["src/**/*.{ts,tsx}"],
    languageOptions: {
      parserOptions: { projectService: true, tsconfigRootDir: import.meta.dirname },
      globals: { ...globals.browser },
    },
    plugins: { "react-hooks": reactHooks },
    rules: {
      ...reactHooks.configs.recommended.rules,
      // We do not run the React Compiler; TanStack Virtual is fine without memoisation.
      "react-hooks/incompatible-library": "off",
      "react-hooks/preserve-manual-memoization": "off",
      // Rule 4 — no escape hatches. A type you can bypass is not a guardrail.
      "@typescript-eslint/no-explicit-any": "error",
      "@typescript-eslint/ban-ts-comment": ["error", { "ts-ignore": true, "ts-nocheck": true, "ts-expect-error": true }],
      "@typescript-eslint/consistent-type-assertions": ["error", { assertionStyle: "never" }],
      "@typescript-eslint/no-non-null-assertion": "error",
      "@typescript-eslint/restrict-template-expressions": ["error", { allowNumber: true }],
      "@typescript-eslint/no-confusing-void-expression": ["error", { ignoreArrowShorthand: true }],
      "@typescript-eslint/no-misused-promises": ["error", { checksVoidReturn: { attributes: false } }],
      // Rule 3 analogue — only the IPC block talks to the Rust core.
      "no-restricted-imports": ["error", {
        paths: [{ name: "@tauri-apps/api/core", message: "Call the Rust core through src/lib/ipc.ts only." }],
      }],
    },
  },
  {
    files: ["src/lib/ipc.ts"],
    rules: { "no-restricted-imports": "off" },
  },
);
