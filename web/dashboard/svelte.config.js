import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({
      pages: '../../internal/dashboard/dist',
      assets: '../../internal/dashboard/dist',
      precompress: false,
      strict: true
    }),
    paths: {
      relative: true
    },
    version: {
      // SvelteKit otherwise defaults this value to Date.now(), which makes the
      // embedded dashboard and release source archive differ on every build.
      name: '1'
    }
  }
};

export default config;
