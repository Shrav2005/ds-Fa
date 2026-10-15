import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'node:fs';

const httpsKeyPath = process.env.HTTPS_KEY_PATH;
const httpsCertPath = process.env.HTTPS_CERT_PATH;
if (Boolean(httpsKeyPath) !== Boolean(httpsCertPath)) {
  throw new Error('Set both HTTPS_KEY_PATH and HTTPS_CERT_PATH to enable HTTPS for the Vite dev server.');
}

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    allowedHosts: ['.trycloudflare.com'],
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:5000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://127.0.0.1:8001',
        changeOrigin: true,
        ws: true,
      },
      '/ice-config': {
        target: 'http://127.0.0.1:8001',
        changeOrigin: true,
      },
    },
    https: httpsKeyPath
      ? {
          key: readFileSync(httpsKeyPath),
          cert: readFileSync(httpsCertPath),
        }
      : undefined,
  },
});
