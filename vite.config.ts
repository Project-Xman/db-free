// SOT: vite-config, dev-server-port, tauri-frontend-build
import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// WHAT:  Vite config for the Tauri webview frontend.
// WHY:   Tauri expects a fixed dev port (1420) and a static dist folder.
// HOW:   tauri.conf.json `build.devUrl` / `frontendDist` point here.
// WHERE: src-tauri/tauri.conf.json
export default defineConfig({
  plugins: [react(), tailwindcss()],
  // Mirrors tsconfig `paths`: Vite resolves `@/…` itself, tsc only type-checks it.
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    watch: { ignored: ["**/src-tauri/**"] },
  },
  build: {
    target: ["es2022", "safari16"],
    sourcemap: false,
  },
});
