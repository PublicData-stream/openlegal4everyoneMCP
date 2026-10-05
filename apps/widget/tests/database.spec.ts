import { test, expect } from '@playwright/test';
test('expanded dataset filters render new records and select capture history', async ({ page }) => {
  await page.goto('/database'); const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
  const datasets = widget.getByLabel('Dataset');
  await expect(datasets.locator('option')).toHaveCount(70);
  await datasets.selectOption('ppc_decision');
  await widget.getByRole('button', { name: 'Search corpus' }).click();
  await expect(widget.locator('article')).toContainText('ppc_decision');
  await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
  await expect(widget.getByLabel('Object content')).toHaveText('first fictional page');
  await expect(widget.getByLabel('History type')).toHaveValue('captures');
  await expect(widget.getByLabel('History type').locator('option[value="revisions"]')).toHaveAttribute('disabled', '');
  await widget.getByRole('button', { name: 'Load history' }).click();
  await expect(widget.getByRole('button', { name: 'Read checkpoint' })).toHaveCount(2);
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  expect(calls[0].arguments.filters.datasets).toEqual(['ppc_decision']);
  expect(calls.at(-1).arguments.kind).toBe('captures');
});
test('fictional database bridge pins content pages and shows HEAD freshness', async ({ page }) => {
  await page.goto('/database'); const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
  await widget.getByLabel('Search expression').fill('fictional');
  await widget.getByRole('button', { name: 'Search corpus' }).click();
  await expect(widget.getByText('Partial corpus coverage', { exact: false })).toBeVisible();
  await expect(widget.getByRole('status').first()).toContainText('다운로드 실패 / failed download');
  await expect(widget.locator('article b')).toHaveCount(0);
  await expect(widget.locator('article')).toContainText('Whole-object query match · illustrative excerpt from body');
  await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
  await expect(widget.getByLabel('Object content')).toHaveText('first fictional page');
  await widget.getByRole('button', { name: 'Next content page' }).click();
  await expect(widget.getByLabel('Object content')).toHaveText('second fictional page');
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  expect(calls.at(-1).arguments.selector).toEqual({ kind: 'capture', id: 'a'.repeat(64) });
  expect(calls.at(-1).arguments.session).toBe('e'.repeat(64));
  await widget.getByRole('button', { name: 'Read HEAD' }).click();
  await expect(widget.getByText('fresh; cache age 10s; TTL 300s; fresh remaining 290s')).toBeVisible();
});
test('fictional checkpoint diff preserves provenance and deletes its handle', async ({ page }) => {
  await page.goto('/database'); const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
  await widget.getByLabel('Search method').selectOption('rg');
  await widget.getByLabel('Search expression').fill('fictional.*');
  await widget.getByRole('button', { name: 'Search corpus' }).click();
  await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
  await widget.getByRole('button', { name: 'Load history' }).click();
  await widget.getByRole('button', { name: 'Use as before' }).first().click();
  await widget.getByRole('button', { name: 'Use as after' }).last().click();
  await widget.getByRole('button', { name: 'Compare checkpoints' }).click();
  await expect(widget.getByLabel('Checkpoint comparison')).toContainText('Before revision r1');
  await expect(widget.getByLabel('Checkpoint comparison')).toContainText('OCR excluded.');
  await widget.getByRole('button', { name: 'Clear comparison' }).click();
  await expect(widget.getByLabel('Checkpoint comparison')).toHaveCount(0);
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  expect(calls[0].name).toBe('database.rg');
  expect(calls.at(-1).name).toBe('text.diff.delete');
});

test('fictional section catalog merges pages and keeps long section IDs with the capture session', async ({ page }) => {
  await page.goto('/database'); const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
  await widget.getByRole('button', { name: 'Search corpus' }).click();
  await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
  await widget.getByRole('button', { name: 'Load more sections' }).click();
  await expect(widget.getByText('2 of 2 section summaries loaded.')).toBeVisible();
  await widget.getByLabel('Content section').selectOption('x'.repeat(256));
  await expect(widget.getByLabel('Object content')).toHaveText('Fictional section content');
  await expect(widget.getByText('2 of 2 section summaries loaded.')).toBeVisible();
  await widget.getByRole('button', { name: 'Load history' }).click();
  await expect(widget.getByText('Catalog entry; no retained capture observation', { exact: false }).first()).toBeVisible();
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  const catalog = calls.find((call: { arguments: { sections_offset?: number } }) => call.arguments.sections_offset === 1);
  expect(catalog.arguments.session).toBe('e'.repeat(64));
  expect(catalog.arguments.selector).toEqual({ kind: 'capture', id: 'a'.repeat(64) });
});

test('provider diagnostics distinguish recheck from recovery and show pending HEAD', async ({ page }) => {
  await page.goto('/database?provider=pending'); const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
  await widget.getByRole('button', { name: 'Search corpus' }).click();
  const admission = widget.getByLabel('Provider admission');
  await expect(admission).toContainText('Operator action required');
  await expect(admission).toContainText('Blocked since: unknown');
  await expect(admission).toContainText('Uncertainty first observed:');
  await expect(admission).toContainText('Recheck timing is not a recovery estimate');
  await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
  await widget.getByRole('button', { name: 'Read HEAD' }).click();
  await expect(widget.getByText('HEAD collection pending:', { exact: false })).toContainText('deferred');
  await expect(widget.getByLabel('Object content')).toHaveCount(0);
  const calls = JSON.parse(await page.locator('#calls').textContent() || '[]');
  const before = calls.length;
  await page.waitForTimeout(500);
  expect(JSON.parse(await page.locator('#calls').textContent() || '[]')).toHaveLength(before);
});

for (const source of ['', '?provider=malformed']) {
  test(`missing or invalid diagnostic keeps local corpus evidence usable (${source || 'older server'})`, async ({ page }) => {
    await page.goto(`/database${source}`); const widget = page.frameLocator('iframe');
    await expect(widget.getByRole('button', { name: 'Search corpus' })).toBeEnabled();
    await widget.getByRole('button', { name: 'Search corpus' }).click();
    await expect(widget.getByLabel('Provider admission')).toContainText('status unknown');
    await expect(widget.getByLabel('Provider admission')).not.toContainText('admission available');
    await widget.getByRole('button', { name: 'Fictional sample statute' }).click();
    await expect(widget.getByLabel('Object content')).toHaveText('first fictional page');
    await expect(widget.getByLabel('Provider admission')).toContainText('status unknown');
  });
}
