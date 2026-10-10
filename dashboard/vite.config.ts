import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// In development the API runs separately (feedwatch api); in Docker it serves this build.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://localhost:8000" } },
});
