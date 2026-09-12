import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateSourceUrl } from '../src/source-offer.ts';

test('source URL retains HTTPS offer text and enforces the byte bound', () => {
  const escaped = 'https://source.example/release?name="widget"&part=server';
  assert.equal(validateSourceUrl(escaped), escaped);
  const prefix = 'https://source.example/';
  assert.equal(validateSourceUrl(prefix + 'a'.repeat(2048 - prefix.length))?.length, 2048);
  assert.equal(validateSourceUrl(prefix + 'a'.repeat(2049 - prefix.length)), null);
  assert.equal(validateSourceUrl(prefix + '가'.repeat(683)), null);
});

test('missing, malformed, non-HTTPS and credential-bearing offers fail closed', () => {
  for (const value of [null, '', '__OPENLEGAL_SOURCE_URL__', '/source', 'http://source.example/', 'javascript:alert(1)', 'https:source.example', 'https:///source.example', 'https://', 'https://user@source.example/', 'https://user:secret@source.example/', ' https://source.example', 'https://source.example/\npath', 'https://source.example/\u0085']) {
    assert.equal(validateSourceUrl(value), null, String(value));
  }
});
