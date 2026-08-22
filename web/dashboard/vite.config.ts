import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, realpathSync, statSync } from 'node:fs';
import { dirname, join, parse } from 'node:path';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig, type Plugin } from 'vite';

const licenseName = /^(?:LICEN[CS]E|COPYING)(?:$|[._-])/i;

function compiledLicenseBundle(): Plugin {
  return {
    name: 'agentos-compiled-license-bundle',
    generateBundle(_options, bundle) {
      const roots = new Set<string>();
      for (const output of Object.values(bundle)) {
        if (output.type !== 'chunk') continue;
        for (const rawID of Object.keys(output.modules)) {
          const id = rawID.split('?', 1)[0];
          if (!id.includes('node_modules')) continue;
          const root = packageRoot(id);
          if (root) roots.add(root);
        }
      }
      const texts = new Map<string, string>();
      const packages = Array.from(roots, (root) => {
        const metadata = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8')) as {
          name?: unknown;
          version?: unknown;
          license?: unknown;
        };
        if (typeof metadata.name !== 'string' || typeof metadata.version !== 'string' || typeof metadata.license !== 'string') {
          throw new Error(`compiled dashboard package at ${root} lacks canonical identity or license metadata`);
        }
        const evidence = readdirSync(root)
          .filter((name) => licenseName.test(name) && statSync(join(root, name)).isFile())
          .sort()
          .map((name) => {
            const body = readFileSync(join(root, name));
            if (body.length === 0 || body.length > 2 * 1024 * 1024) {
              throw new Error(`license evidence for ${metadata.name} is empty or oversized`);
            }
            const sha256 = createHash('sha256').update(body).digest('hex');
            texts.set(sha256, body.toString('utf8'));
            return { file: name, sha256 };
          });
        if (evidence.length === 0) {
          throw new Error(`compiled dashboard package ${metadata.name}@${metadata.version} lacks root license evidence`);
        }
        return { name: metadata.name, version: metadata.version, declared_license: metadata.license, evidence };
      }).sort((left, right) => left.name.localeCompare(right.name) || left.version.localeCompare(right.version));
      if (packages.length === 0) {
        throw new Error('dashboard build unexpectedly contains no compiled third-party packages');
      }
      const lockfile = readFileSync(join(process.cwd(), 'pnpm-lock.yaml'));
      const licenseTexts = Array.from(texts, ([sha256, text]) => ({ sha256, text })).sort((left, right) => left.sha256.localeCompare(right.sha256));
      this.emitFile({
        type: 'asset',
        fileName: 'THIRD_PARTY_LICENSES.json',
        source: JSON.stringify({
          schema_version: 1,
          lockfile_sha256: createHash('sha256').update(lockfile).digest('hex'),
          packages,
          license_texts: licenseTexts
        }, null, 2) + '\n'
      });
    }
  };
}

function packageRoot(moduleID: string): string | null {
  let current: string;
  try {
    current = dirname(realpathSync(moduleID));
  } catch {
    return null;
  }
  const filesystemRoot = parse(current).root;
  while (current !== filesystemRoot) {
    const metadata = join(current, 'package.json');
    try {
      if (statSync(metadata).isFile()) return current;
    } catch {
      // Continue toward the package root.
    }
    current = dirname(current);
  }
  return null;
}

export default defineConfig({
  plugins: [sveltekit(), compiledLicenseBundle()],
  build: {
    sourcemap: false
  }
});
