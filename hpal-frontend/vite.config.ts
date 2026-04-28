/**
 * Vite Configuration
 * 
 * Build and dev server setup for React + TypeScript frontend.
 */

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const apiBaseUrl = process.env.VITE_API_BASE_URL || "http://localhost:8000";
const proxyTarget = process.env.VITE_PROXY_TARGET || apiBaseUrl;

export default defineConfig({
  plugins: [react()],
  server: {
    host: "localhost",
    port: 5173,
    strictPort: false,
    open: false,
    proxy: {
      "/api": {
        target: proxyTarget,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      "/contracts": {
        target: proxyTarget,
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    target: "es2020",
    minify: "esbuild",
    sourcemap: false,
  },
  define: {
    "process.env.VITE_API_BASE_URL": JSON.stringify(
      apiBaseUrl
    ),
  },
});
