import type { DashboardIdentity } from './types';
import { DisplayError } from './display-error.ts';

const sessionKey = 'agentos.dashboard.session';
let sessionToken = '';

export class APIError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'APIError';
    this.status = status;
  }
}

export function isDashboardSessionRejection(error: unknown): boolean {
  return error instanceof APIError && error.status === 401;
}

export function emptyJSONPost(): RequestInit {
  return { method: 'POST', headers: { 'Content-Type': 'application/json' } };
}

function currentBootstrapToken(): string {
  const params = new URLSearchParams(window.location.hash.slice(1));
  return params.get('bootstrap') ?? '';
}

function clearBootstrapFragment(): void {
  history.replaceState(null, '', window.location.pathname + window.location.search);
}

export async function connect(): Promise<DashboardIdentity> {
  sessionToken = sessionStorage.getItem(sessionKey) ?? '';
  const bootstrapToken = currentBootstrapToken();
  if (bootstrapToken) {
    const response = await fetch('/api/session', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bootstrap_token: bootstrapToken })
    });
    const body = await decode<{ session_token: string }>(response);
    sessionToken = body.session_token;
    sessionStorage.setItem(sessionKey, sessionToken);
    clearBootstrapFragment();
  }
  if (!sessionToken) {
    throw new DisplayError('error.sessionRequired', 'Launch the dashboard with `agentos` to establish a local session.');
  }
  try {
    try {
      await api<void>('/api/session/ack', { method: 'POST' });
    } catch {
      // A lost acknowledgement response is recoverable: the stored session
      // remains usable and the next dashboard load retries acknowledgement.
    }
    return await api<DashboardIdentity>('/api/dashboard');
  } catch (error) {
    if (isDashboardSessionRejection(error)) {
      sessionStorage.removeItem(sessionKey);
      sessionToken = '';
    }
    throw error;
  }
}

export async function api<T>(path: string, options: RequestInit = {}): Promise<T> {
  if (!sessionToken) {
    sessionToken = sessionStorage.getItem(sessionKey) ?? '';
  }
  const headers = new Headers(options.headers);
  headers.set('Accept', 'application/json');
  headers.set('Authorization', `Bearer ${sessionToken}`);
  if (options.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }
  const response = await fetch(path, { ...options, headers });
  return decode<T>(response);
}

export async function verifiedDownload(path: string): Promise<{ body: ArrayBuffer; sha256: string }> {
  if (!sessionToken) {
    sessionToken = sessionStorage.getItem(sessionKey) ?? '';
  }
  const response = await fetch(path, {
    headers: { Accept: 'application/json', Authorization: `Bearer ${sessionToken}` }
  });
  if (!response.ok) {
    await decode<never>(response);
  }
  const expected = response.headers.get('X-AgentOS-SHA256') ?? '';
  const body = await response.arrayBuffer();
  await verifySHA256(body, expected);
  return { body, sha256: expected };
}

export async function verifySHA256(body: ArrayBuffer, expected: string): Promise<void> {
  if (!/^[0-9a-f]{64}$/.test(expected)) {
    throw new DisplayError('error.evidenceChecksumMissing', 'The evidence response did not include a valid SHA-256 checksum.');
  }
  const digest = await crypto.subtle.digest('SHA-256', body);
  const actual = Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, '0')).join('');
  if (actual !== expected) {
    throw new DisplayError('error.evidenceChecksumMismatch', 'The evidence response did not match its SHA-256 checksum.');
  }
}

async function decode<T>(response: Response): Promise<T> {
  const body = (await response.json().catch(() => ({ error: `HTTP ${response.status}` }))) as T & { error?: string };
  if (!response.ok) {
    throw new APIError(response.status, body.error ?? `HTTP ${response.status}`);
  }
  return body;
}

export function identifier(prefix: string): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return `${prefix}-${Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('')}`;
}
