import { test, expect } from '@playwright/test';
import { readFile } from 'node:fs/promises';
const sourceUrl = 'https://source.example/release?name="widget"&part=server';
test('host bridge searches, paginates, changes source, and opens escaped details', async ({ page }) => {
  const external: string[] = [];
  page.on('request', request => { if (!request.url().startsWith('http://127.0.0.1:4173/')) external.push(request.url()); });
  await page.goto('/');
  const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search', exact: true })).toBeEnabled();
  await widget.getByLabel('Source', { exact: true }).selectOption('layout_b');
  await widget.getByLabel('Require fresh results').check();
  await widget.getByRole('button', { name: 'Search', exact: true }).click();
  await expect(widget.getByText('Page 1 · 7 records')).toBeVisible();
  await expect(widget.getByRole('button', { name: 'Previous' })).toBeDisabled();
  await widget.getByRole('button', { name: 'Next', exact: true }).click();
  await expect(widget.getByText('Page 2 · 7 records')).toBeVisible();
  await expect(widget.getByRole('button', { name: 'Next', exact: true })).toBeDisabled();
  await widget.getByRole('button', { name: 'Synthetic record 6', exact: true }).focus();
  await page.keyboard.press('Enter');
  await expect(widget.getByRole('heading', { name: 'Synthetic record 6' })).toBeFocused();
  await expect(widget.getByText('Synthetic body 6. <b>Plain source text</b>')).toBeVisible();
  await expect(widget.locator('article b')).toHaveCount(0);
  await expect(widget.getByText('Synthetic · layout_b / demo-6')).toBeVisible();
  await widget.getByText('Source and processing details').click();
  await expect(widget.getByText('synthetic:fixture')).toBeVisible();
  await expect(widget.getByText('1.0.0', { exact: true })).toBeVisible();
  await widget.getByRole('button', { name: 'Back to results' }).click();
  await expect(widget.getByText('Page 2 · 7 records')).toBeVisible();
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  expect(calls[0].arguments).toEqual({ source: 'layout_b', query: '', page: 0, page_size: 5, fresh_only: true });
  expect(calls[2]).toEqual({ name: 'demo_get_record', arguments: { source: 'layout_b', id: 'demo-6', fresh_only: true } });
  expect(external).toEqual([]);
});
test('loading, stale, empty, tool failure and malformed results remain usable', async ({ page }) => {
  await page.goto('/');
  const widget = page.frameLocator('iframe');
  const input = widget.getByLabel('Search records', { exact: true });
  for (const query of ['slow', 'stale', 'empty', 'error', 'malformed']) {
    await input.fill(query);
    await widget.getByRole('button', { name: 'Search', exact: true }).click();
    if (query === 'slow') { await expect(widget.getByRole('status')).toHaveText('Loading records…'); await expect(input).toBeDisabled(); }
    if (query === 'stale') await expect(widget.getByText('Stale fallback when returned · age at retrieval 70s')).toHaveCount(5);
    if (query === 'empty') await expect(widget.getByText('No records match this search.')).toBeVisible();
    if (query === 'error') { await expect(widget.getByRole('alert')).toContainText('Try again'); await expect(widget.getByText('Private internal failure')).toHaveCount(0); }
    if (query === 'malformed') await expect(widget.getByRole('alert')).toContainText('unsupported record response');
    await expect(input).toBeEnabled();
  }
});
test('initial rendering preserves each source and freshness, malformed initial data fails safely', async ({ page }) => {
  await page.goto('/?initial');
  const widget = page.frameLocator('iframe');
  await expect(widget.getByText('Synthetic · layout_a / demo-1')).toBeVisible();
  await expect(widget.getByText('Synthetic · layout_b / demo-1')).toBeVisible();
  await expect(widget.getByText('Stale fallback when returned · age at retrieval 70s')).toBeVisible();
  await expect(widget.getByText('Fresh when returned · age at retrieval 0s')).toBeVisible();
  await page.goto('/?malformed');
  await expect(widget.getByRole('alert')).toContainText('unsupported record response');
});

test('oversized UTF-8 search reports a local error without calling the host', async ({ page }) => {
  await page.goto('/');
  const widget = page.frameLocator('iframe');
  await widget.getByLabel('Search records', { exact: true }).fill('가'.repeat(86));
  await widget.getByRole('button', { name: 'Search', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('256 UTF-8 bytes');
  expect(JSON.parse(await page.locator('#calls').textContent() || '[]')).toEqual([]);
});

test('visible source offer retains escaped URL, full license, and click-only host navigation', async ({ page }) => {
  const external: string[] = [];
  page.on('request', request => { if (!request.url().startsWith('http://127.0.0.1:4173/')) external.push(request.url()); });
  await page.goto('/');
  const widget = page.frameLocator('iframe');
  const source = widget.getByRole('region', { name: 'Corresponding source' });
  await expect(source.getByRole('button', { name: 'Get source code' })).toBeEnabled();
  await expect(source.getByLabel('Source code URL')).toHaveValue(sourceUrl);
  await expect(widget.getByText('Copyright © 2026 PiQuark6046.', { exact: false })).toBeVisible();
  await expect(widget.getByText('This program comes with no warranty.', { exact: false })).toBeVisible();
  await expect(widget.locator('#license-text')).not.toBeVisible();
  await widget.getByText('Read the full GNU AGPLv3 license', { exact: true }).click();
  await expect(widget.locator('#license-text')).toBeVisible();
  expect(await widget.locator('#license-text').textContent()).toBe(await readFile('../../LICENSE', 'utf8'));
  expect(await widget.locator('meta[name="openlegal-source-url"]').getAttribute('content')).toBe(sourceUrl);
  expect(await widget.locator('meta[name="openlegal-source-url"]').evaluate(element => element.attributes.length)).toBe(2);
  expect(JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual([]);
  await source.getByRole('button', { name: 'Get source code' }).click();
  await expect(source.getByText('Source link sent to the host.')).toBeVisible();
  expect(JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual([sourceUrl]);
  expect(external).toEqual([]);
});

for (const mode of ['unsupported', 'disconnected', 'denied', 'throws']) {
  test(`source URL stays selectable when host is ${mode}`, async ({ page }) => {
    await page.goto(`/?${mode}`);
    const widget = page.frameLocator('iframe');
    const source = widget.getByRole('region', { name: 'Corresponding source' });
    const button = source.getByRole('button', { name: 'Get source code' });
    if (mode === 'unsupported' || mode === 'disconnected') {
      if (mode === 'unsupported') await expect(widget.getByRole('button', { name: 'Search', exact: true })).toBeEnabled();
      await expect(button).toBeDisabled();
      expect(JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual([]);
    } else {
      await button.click();
      await expect(source.getByText(mode === 'denied' ? 'The host declined to open the source. Copy the URL below.' : 'The source link could not be opened. Copy the URL below.')).toBeVisible();
      expect(JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual([sourceUrl]);
    }
    const input = source.getByLabel('Source code URL');
    await expect(input).toHaveValue(sourceUrl);
    await input.focus();
    expect(await input.evaluate((element: HTMLInputElement) => element.value.substring(element.selectionStart ?? 0, element.selectionEnd ?? 0))).toBe(sourceUrl);
    await expect(widget.getByText('Read the full GNU AGPLv3 license', { exact: true })).toBeVisible();
  });
}

for (const mode of ['invalid-source', 'duplicate-source']) {
  test(`${mode} metadata prevents navigation`, async ({ page }) => {
    await page.goto(`/?${mode}`);
    const widget = page.frameLocator('iframe');
    await expect(widget.getByText('The source URL is unavailable or invalid. Ask the operator for the corresponding source.')).toBeVisible();
    await expect(widget.getByRole('button', { name: 'Get source code' })).toHaveCount(0);
    expect(JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual([]);
  });
}
