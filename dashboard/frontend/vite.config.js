import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Trong Docker: BACKEND_INTERNAL_URL=http://dashboard_backend:4000
// Trên máy dev (chỉ npm run dev): mặc định http://127.0.0.1:4000
const backendInternal = process.env.BACKEND_INTERNAL_URL || 'http://127.0.0.1:4000'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/socket.io': {
        target: backendInternal,
        changeOrigin: true,
        ws: true,
      },
    },
  },
})
