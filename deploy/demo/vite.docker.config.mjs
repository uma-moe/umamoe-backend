import { readFileSync } from 'node:fs';
import { mergeConfig } from 'vite';
import frontendConfig from './vite.config.ts';

// Mounted beside the frontend's own config, preserving its plugins and aliases.
export default async (environment) => mergeConfig(await frontendConfig(environment), {
  server: {
    allowedHosts: ['frontend'],
    watch: { usePolling: true },
    proxy: JSON.parse(readFileSync('/config/proxy.json', 'utf8')),
  },
});
