import type { DashboardIdentity } from './types';

const sessionKey = 'agentos.dashboard.session';
let sessionToken = '';

export class APIError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
    this.name = 'APIError';
  }
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
    clearBootstrapFragment();
    const body = await decode<{ session_token: string }>(response);
    sessionToken = body.session_token;
    sessionStorage.setItem(sessionKey, sessionToken);
  }
  if (!sessionToken) {
    throw new Error('Launch the dashboard with `agentos` to establish a local session.');
  }
  try {
    return await api<DashboardIdentity>('/api/dashboard');
  } catch (error) {
    sessionStorage.removeItem(sessionKey);
    sessionToken = '';
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
