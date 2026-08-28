import assert from 'node:assert/strict';
import test from 'node:test';
import { english, formatDisplayMessage, resolveDisplayLocale } from './i18n.ts';
import { spanish } from './i18n/es.ts';
import { simplifiedChinese } from './i18n/zh-CN.ts';

test('resolves only allowlisted locale families with deterministic English fallback', () => {
  assert.equal(resolveDisplayLocale(['en-US']), 'en');
  assert.equal(resolveDisplayLocale(['fr-FR', 'EN-gb']), 'en');
  assert.equal(resolveDisplayLocale(['fr-FR']), 'en');
  assert.equal(resolveDisplayLocale(['en<script>']), 'en');
  assert.equal(resolveDisplayLocale(['']), 'en');
});

test('resolves supported Spanish and Simplified Chinese locale families', () => {
  assert.equal(resolveDisplayLocale(['es']), 'es');
  assert.equal(resolveDisplayLocale(['es-MX']), 'es');
  assert.equal(resolveDisplayLocale(['es-419']), 'es');
  assert.equal(resolveDisplayLocale(['zh']), 'zh-CN');
  assert.equal(resolveDisplayLocale(['zh-CN']), 'zh-CN');
  assert.equal(resolveDisplayLocale(['zh-SG']), 'zh-CN');
  assert.equal(resolveDisplayLocale(['zh-Hans']), 'zh-CN');
  assert.equal(resolveDisplayLocale(['zh-Hans-CN']), 'zh-CN');
  assert.equal(resolveDisplayLocale(['zh-TW', 'es-ES']), 'es');
  assert.equal(resolveDisplayLocale(['zh-Hant', 'en-US']), 'en');
});

test('keeps every translated interpolation contract identical to English', () => {
  const placeholders = (message: string) => [...message.matchAll(/\{([a-z][a-z0-9_]*)\}/gi)].map((match) => match[1]).sort();
  for (const id of Object.keys(english) as (keyof typeof english)[]) {
    assert.deepEqual(placeholders(spanish[id]), placeholders(english[id]), `Spanish placeholders for ${id}`);
    assert.deepEqual(
      placeholders(simplifiedChinese[id]),
      placeholders(english[id]),
      `Simplified Chinese placeholders for ${id}`
    );
  }
});

test('formats typed display messages with exact interpolation values', () => {
  assert.equal(
    formatDisplayMessage('en', 'overview.organizationCounts', { missions: 1, goals: 2, teams: 3, agents: 4 }),
    '1 Missions · 2 Goals · 3 Teams · 4 Agents'
  );
  assert.equal(formatDisplayMessage('en', 'header.refresh'), 'Refresh');
  assert.equal(formatDisplayMessage('es', 'header.refresh'), 'Actualizar');
  assert.equal(formatDisplayMessage('zh-CN', 'header.refresh'), '刷新');
  assert.equal(
    formatDisplayMessage('en', 'organization.assignedTo', { type: 'AGENT', id: 'agent-1' }),
    'Assigned to AGENT agent-1'
  );
  assert.equal(
    formatDisplayMessage('en', 'task.fieldBounds', { description: 'Exact evidence', minimum: 1, maximum: 64 }),
    'Exact evidence; 1 to 64 UTF-8 bytes'
  );
  assert.equal(
    formatDisplayMessage('es', 'task.fieldBounds', { description: 'Evidencia exacta', minimum: 1, maximum: 64 }),
    'Evidencia exacta; de 1 a 64 bytes UTF-8'
  );
  assert.equal(
    formatDisplayMessage('zh-CN', 'task.fieldBounds', { description: '精确证据', minimum: 1, maximum: 64 }),
    '精确证据；1 至 64 个 UTF-8 字节'
  );
});

test('fails closed when interpolation values are missing or unexpected', () => {
  assert.throws(
    () => formatDisplayMessage('en', 'overview.organizationCounts', { missions: 1, goals: 2, teams: 3 }),
    /invalid display message values/
  );
  assert.throws(() => formatDisplayMessage('en', 'header.refresh', { authority: 'APPROVE' }), /invalid display message values/);
});
