import assert from 'node:assert/strict';
import test from 'node:test';
import { formatDisplayMessage, resolveDisplayLocale } from './i18n.ts';

test('resolves only allowlisted locale families with deterministic English fallback', () => {
  assert.equal(resolveDisplayLocale(['en-US']), 'en');
  assert.equal(resolveDisplayLocale(['fr-FR', 'EN-gb']), 'en');
  assert.equal(resolveDisplayLocale(['fr-FR']), 'en');
  assert.equal(resolveDisplayLocale(['en<script>']), 'en');
  assert.equal(resolveDisplayLocale(['']), 'en');
});

test('formats typed display messages with exact interpolation values', () => {
  assert.equal(
    formatDisplayMessage('en', 'overview.organizationCounts', { missions: 1, goals: 2, teams: 3, agents: 4 }),
    '1 Missions · 2 Goals · 3 Teams · 4 Agents'
  );
  assert.equal(formatDisplayMessage('en', 'header.refresh'), 'Refresh');
  assert.equal(
    formatDisplayMessage('en', 'organization.assignedTo', { type: 'AGENT', id: 'agent-1' }),
    'Assigned to AGENT agent-1'
  );
  assert.equal(
    formatDisplayMessage('en', 'task.fieldBounds', { description: 'Exact evidence', minimum: 1, maximum: 64 }),
    'Exact evidence; 1 to 64 UTF-8 bytes'
  );
});

test('fails closed when interpolation values are missing or unexpected', () => {
  assert.throws(
    () => formatDisplayMessage('en', 'overview.organizationCounts', { missions: 1, goals: 2, teams: 3 }),
    /invalid display message values/
  );
  assert.throws(() => formatDisplayMessage('en', 'header.refresh', { authority: 'APPROVE' }), /invalid display message values/);
});
