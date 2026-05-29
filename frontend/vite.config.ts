import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// In dev, proxy /api to the FastAPI backend (port 8077) so the SPA uses
// same-origin relative URLs in both dev and prod (where FastAPI serves dist/).
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8077",
        changeOrigin: true,
      },
    },
  },
});
