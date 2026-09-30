import { test, expect, type Page } from '@playwright/test';
const calls = async (page: Page) => JSON.parse(await page.locator('#calls').textContent() || '[]') as { name: string; arguments: Record<string, unknown> }[];
async function open(page: Page, suffix = '') {
  await page.goto(`/diff${suffix}`);
  const widget = page.frameLocator('iframe');
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeVisible();
  return widget;
}
test('compares exact pasted text through the host, renders the actual diff, and keeps source content inert', async ({ page }) => {
  const external: string[] = [];
  page.on('request', request => { if (!request.url().startsWith('http://127.0.0.1:4173/')) external.push(request.url()); });
  const widget = await open(page);
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  await widget.getByLabel('Before text', { exact: true }).fill('가 <b>before</b>\n');
  await widget.getByLabel('After text', { exact: true }).fill('😀 <img src="https://evil.example/x">\n');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByText('1 added lines · 1 deleted lines')).toBeVisible();
  await expect(widget.locator('.diff-fragment')).toContainText('<b>before</b>');
  await expect(widget.locator('.diff-fragment')).toContainText('<img src="https://evil.example/x">');
  await expect(widget.locator('.diff-fragment img, .diff-fragment b')).toHaveCount(0);
  const invoked = await calls(page);
  expect(invoked[0]).toMatchObject({ name: 'text.diff', arguments: { before: '가 <b>before</b>\n', after: '😀 <img src="https://evil.example/x">\n' } });
  expect(invoked[1].name).toBe('text.attachment.delete');
  expect(invoked[2].name).toBe('text.diff.page');
  expect(external).toEqual([]);
  await expect(widget.locator('.diff-fragment button')).toHaveCount(0);
  await widget.getByText('Read the full GNU AGPLv3 license', { exact: true }).click();
  await expect(widget.locator('#license-text')).toBeVisible();
  await widget.getByRole('button', { name: 'Get source code' }).click();
  await expect.poll(async () => JSON.parse(await page.locator('#links').textContent() || '[]')).toEqual(['https://source.example/release']);
  await widget.getByText('Third-party dependency notices', { exact: true }).click();
  await expect(widget.locator('#dependency-notices')).toContainText('@modelcontextprotocol/ext-apps@2.0.0 (MIT)');
});
test('existing handles load source chunks in order for editing, preserving CR until explicit LF conversion', async ({ page }) => {
  const widget = await open(page, '?initial');
  const load = widget.getByRole('button', { name: 'Load original texts for editing' });
  await expect(load).toBeEnabled();
  await load.click();
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveAttribute('readonly', '');
  await expect(widget.getByText('Original CR bytes are preserved.', { exact: false })).toHaveCount(2);
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  const first = (await calls(page)).find(call => call.name === 'text.diff');
  expect(first?.arguments.before).toBe('가\r\nOriginal before\r끝');
  expect(first?.arguments.after).toBe('😀\r\nOriginal after\n');
  await widget.getByRole('button', { name: 'Edit before as LF' }).click();
  await expect(widget.getByLabel('Before text', { exact: true })).not.toHaveAttribute('readonly', '');
  await expect(widget.getByRole('heading', { name: 'Previous comparison' })).toBeVisible();
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  const invoked = (await calls(page)).filter(call => call.name === 'text.diff');
  expect(invoked[1].arguments.before).toBe('가\nOriginal before\n끝');
  const all = await calls(page);
  for (let index = 0; index < all.length; index++) if (all[index].name === 'text.diff') expect(all[index - 1].name).toBe('text.diff.delete');
});
test('UTF-8 uploads keep BOM and CRLF; invalid bytes and oversized lines never call comparison', async ({ page }) => {
  const widget = await open(page);
  const upload = widget.getByLabel('Load before UTF-8 file');
  await upload.setInputFiles({ name: 'before.txt', mimeType: 'text/plain', buffer: Buffer.from('\uFEFF가\r\n') });
  await expect(widget.getByRole('button', { name: 'Edit before as LF' })).toBeVisible();
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  expect((await calls(page)).find(call => call.name === 'text.diff')?.arguments.before).toBe('\uFEFF가\r\n');
  const count = (await calls(page)).filter(call => call.name === 'text.diff').length;
  await upload.setInputFiles({ name: 'invalid.txt', mimeType: 'text/plain', buffer: Buffer.from([0xc3, 0x28]) });
  await expect(widget.getByRole('alert')).toContainText('invalid byte sequences');
  await upload.setInputFiles({ name: 'long.txt', mimeType: 'text/plain', buffer: Buffer.from('가'.repeat(5462)) });
  await expect(widget.getByRole('alert')).toContainText('16 KiB');
  expect((await calls(page)).filter(call => call.name === 'text.diff')).toHaveLength(count);
});
test('bounded pages near line 100,000 retain source ranges while local gutter allocation stays small', async ({ page }) => {
  const widget = await open(page, '?pages');
  await expect(widget.getByText('Page 1 of 2', { exact: true })).toBeVisible();
  await expect(widget.locator('.diff-fragment .metadata')).toContainText('99998–99998');
  expect(await widget.locator('.diff-fragment tr').count()).toBeLessThan(20);
  const numbers = await widget.locator('.diff-fragment [data-line-num]').allTextContents();
  expect(numbers.every(number => number === '1')).toBe(true);
  await widget.getByRole('button', { name: 'Next page', exact: true }).click();
  await expect(widget.getByText('Page 2 of 2', { exact: true })).toBeVisible();
  await expect(widget.locator('.diff-fragment .metadata')).toContainText('100000–100000');
  await widget.getByLabel('Layout', { exact: true }).selectOption('unified');
  await expect(widget.locator('.unified-diff-view')).toBeVisible();
  await widget.getByLabel('Layout', { exact: true }).selectOption('split');
  await expect(widget.locator('.split-diff-view')).toBeVisible();
  await page.setViewportSize({ width: 500, height: 900 });
  await widget.getByLabel('Layout', { exact: true }).selectOption('auto');
  await expect(widget.locator('.unified-diff-view')).toBeVisible();
});
test('Clear waits for deletion and a failed deletion remains retryable without claiming completion', async ({ page }) => {
  const widget = await open(page, '?initial&deletefail');
  await expect(widget.locator('.diff-fragment')).toBeVisible();
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('Clear has not completed');
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toBeVisible();
  await expect(widget.getByText('Private internal failure', { exact: false })).toHaveCount(0);
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('');
  expect((await calls(page)).filter(call => call.name === 'text.diff.delete')).toHaveLength(2);
});
test('Clear stays disabled through cancellation until a late successful handle is deleted', async ({ page }) => {
  const widget = await open(page);
  await widget.getByLabel('Before text', { exact: true }).fill('slow');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('status')).toHaveText('Comparing texts…');
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeDisabled();
  await page.evaluate(() => window.dispatchEvent(new Event('cancel-comparison')));
  await expect(widget.getByRole('alert')).toContainText('cancelled');
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeDisabled();
  await expect(widget.getByRole('alert')).toContainText('retained comparison was deleted');
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeEnabled();
  expect((await calls(page)).filter(call => call.name === 'text.diff.delete')).toHaveLength(1);
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  await open(page, '?deletefail');
  await widget.getByLabel('Before text', { exact: true }).fill('slow');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('status')).toHaveText('Comparing texts…');
  await page.evaluate(() => window.dispatchEvent(new Event('cancel-comparison')));
  await expect(widget.getByRole('alert')).toContainText('cancelled comparison could not be deleted');
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('');
  expect((await calls(page)).filter(call => call.name === 'text.diff.delete')).toHaveLength(2);
});
test('initial supplied pairs and empty comparisons work; malformed data and expiry fail visibly', async ({ page }) => {
  const widget = await open(page, '?pair');
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('<b>before</b>\n');
  await expect(widget.locator('.diff-fragment')).toBeVisible();
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  expect((await calls(page)).filter(call => call.name === 'text.diff')).toHaveLength(1);
  await open(page);
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByText('The supplied texts are identical.')).toBeVisible();
  expect((await calls(page)).filter(call => call.name === 'text.diff.page')).toHaveLength(0);
  await open(page, '?malformed');
  await expect(widget.getByRole('alert')).toContainText('unsupported comparison response');
  await open(page, '?initial&badpage');
  await expect(widget.getByRole('alert')).toContainText('unsupported comparison response');
  await expect(widget.locator('.diff-fragment')).toHaveCount(0);
  await open(page, '?expired');
  await expect(widget.getByText('This comparison has expired.', { exact: false })).toBeVisible();
  await expect(widget.getByRole('button', { name: 'Load original texts for editing' })).toBeDisabled();
  expect((await calls(page)).filter(call => call.name === 'text.diff.page')).toHaveLength(0);
});
test('clear invalidates a page response still in flight', async ({ page }) => {
  const widget = await open(page, '?initial&slowpage');
  await expect(widget.getByRole('status')).toHaveText('Loading comparison page…');
  await expect(widget.getByLabel('View', { exact: true })).toBeDisabled();
  await expect(widget.getByRole('button', { name: 'Load original texts for editing' })).toBeDisabled();
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  await page.waitForTimeout(500);
  await expect(widget.locator('.diff-fragment')).toHaveCount(0);
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('');
});

test('uses supplied Rust scalar ranges without browser rediff, including CJK and visible newline changes', async ({ page }) => {
  const widget = await open(page, '?initial&highlights');
  await expect(widget.locator('.split-diff-view')).toBeVisible();
  // Both lines share this emoji. The fixture intentionally marks it and the CR/LF
  // while leaving other unequal characters unmarked, making browser rediff visible.
  await expect(widget.locator('.removed .inline-change')).toHaveText(['😀', '␍␊']);
  await expect(widget.locator('.added .inline-change')).toHaveCount(0);
  await expect(widget.locator('.no-final-newline')).toHaveText('\\ No newline at end of file');
  await expect(widget.locator('.diff-fragment')).toContainText('가é漢字A');
  await widget.getByLabel('Layout', { exact: true }).selectOption('unified');
  await expect(widget.locator('.removed .inline-change')).toHaveText(['😀', '␍␊']);
  await page.emulateMedia({ colorScheme: 'dark' });
  await expect(widget.locator('.diff-fragment')).toHaveAttribute('data-theme', 'dark');
  await page.emulateMedia({ colorScheme: 'light' });
  await expect(widget.locator('.diff-fragment')).toHaveAttribute('data-theme', 'light');
});
test('missing or invalid server highlights fail visibly without rendering a fallback diff', async ({ page }) => {
  for (const malformed of ['missinghighlights', 'badhighlights']) {
    const widget = await open(page, `?initial&${malformed}`);
    await expect(widget.getByRole('alert')).toContainText('unsupported comparison response');
    await expect(widget.locator('.diff-fragment')).toHaveCount(0);
  }
});

test('opening a snapshot handle shows its origin and supplied-text recomparison drops it', async ({ page }) => {
  const widget = await open(page, '?initial&origin');
  const origin = widget.getByRole('complementary', { name: 'Retained snapshot comparison' });
  await expect(origin).toContainText('layout_a / demo-1');
  await expect(origin).toContainText('a'.repeat(64));
  await widget.getByRole('button', { name: 'Load original texts for editing' }).click();
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(origin).toHaveCount(0);
  const invoked = await calls(page);
  expect(invoked.find(call => call.name === 'text.diff')?.arguments).not.toHaveProperty('origin');
  expect(invoked.filter(call => call.name === 'text.diff.delete')).toHaveLength(1);
});

for (const deletionFails of [false, true]) {
  test(`supplied-text responses cannot inject history origin${deletionFails ? ' when cleanup needs retry' : ''}`, async ({ page }) => {
    const widget = await open(page, `?injectorigin${deletionFails ? '&deletefail' : ''}`);
    await widget.getByLabel('Before text', { exact: true }).fill('User edited before');
    await widget.getByLabel('After text', { exact: true }).fill('User edited after');
    await widget.getByRole('button', { name: 'Compare', exact: true }).click();
    await expect(widget.getByRole('alert')).toContainText('unexpected historical metadata');
    await expect(widget.getByRole('region', { name: 'Comparison result', exact: true })).toHaveCount(0);
    await expect(widget.getByRole('complementary', { name: 'Retained snapshot comparison' })).toHaveCount(0);
    await expect(widget.getByText('forged-history', { exact: false })).toHaveCount(0);
    if (deletionFails) {
      await expect(widget.getByRole('alert')).toContainText('Retry Clear');
      await widget.getByRole('button', { name: 'Clear', exact: true }).click();
      await expect(widget.getByRole('alert')).toHaveCount(0);
    }
    await expect.poll(async () => (await calls(page)).filter(call => call.name === 'text.diff.delete').length).toBe(deletionFails ? 2 : 1);
    const invoked = await calls(page);
    expect(invoked.some(call => call.name === 'text.diff.page')).toBe(false);
  });
}

test('initial supplied pairs cannot claim historical origin and retain only cleanup authority', async ({ page }) => {
  const widget = await open(page, '?pair&origin');
  await expect(widget.getByRole('alert')).toContainText('unexpected historical metadata');
  await expect(widget.getByRole('complementary', { name: 'Retained snapshot comparison' })).toHaveCount(0);
  await expect(widget.getByRole('region', { name: 'Comparison result', exact: true })).toHaveCount(0);
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('alert')).toHaveCount(0);
  await expect.poll(async () => (await calls(page)).filter(call => call.name === 'text.diff.delete').length).toBe(1);
  const invoked = await calls(page);
  expect(invoked.some(call => call.name === 'text.diff.page')).toBe(false);
});

test('patch file workflow uploads exact UTF-8 and cleans all temporary handles', async ({ page }) => {
  const widget = await open(page);
  await widget.getByLabel('Load target file', { exact: true }).setInputFiles({ name: 'target.txt', mimeType: 'text/plain', buffer: Buffer.from('\uFEFF한\r\n끝') });
  await widget.getByLabel('Load patch file', { exact: true }).setInputFiles({ name: 'change.patch', mimeType: 'text/plain', buffer: Buffer.from('--- a\n+++ b\n@@ -1,2 +1,2 @@\n \uFEFF한\r\n-끝\n\\ No newline at end of file\n+끝!\n\\ No newline at end of file\n') });
  expect((await calls(page)).length).toBe(0);
  await widget.getByRole('button', { name: 'Apply patch', exact: true }).click();
  await expect(widget.getByLabel('Patched text', { exact: true })).toContainText('끝!');
  await expect(widget.getByRole('button', { name: 'Apply patch', exact: true })).toBeEnabled();
  const invoked = await calls(page);
  expect(invoked.find(x => x.name === 'text.attachment.upload')?.arguments.chunk).toBe('\uFEFF한\r\n끝');
  expect(invoked.filter(x => x.name === 'text.attachment.delete')).toHaveLength(3);
  await widget.getByRole('button', { name: 'Clear patch', exact: true }).click();
  await expect(widget.getByLabel('Patched text', { exact: true })).toHaveCount(0);
});

test('patch cleanup failures keep a retryable handle', async ({ page }) => {
  const widget = await open(page, '?patchdeletefail');
  await widget.getByLabel('Patch target', { exact: true }).fill('a');
  await widget.getByRole('button', { name: 'Apply patch', exact: true }).click();
  await expect(widget.getByText('Some temporary attachments could not be deleted.', { exact: false })).toBeVisible();
  await widget.getByRole('button', { name: 'Clear patch', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Apply patch', exact: true })).toBeEnabled();
  expect((await calls(page)).filter(x => x.name === 'text.attachment.delete')).toHaveLength(4);
});

test('unused comparison patches are deleted promptly and failed cleanup remains retryable', async ({ page }) => {
  const widget = await open(page, '?patchdeletefail');
  await widget.getByLabel('Before text', { exact: true }).fill('old');
  await widget.getByLabel('After text', { exact: true }).fill('new');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('unused patch could not be deleted');
  await expect(widget.locator('.diff-fragment')).toBeVisible();
  expect((await calls(page)).filter(x => x.name === 'text.attachment.delete')).toHaveLength(1);
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('alert')).toHaveCount(0);
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  const invoked = await calls(page);
  expect(invoked.filter(x => x.name === 'text.attachment.delete')).toHaveLength(2);
  expect(invoked.filter(x => x.name === 'text.diff.delete')).toHaveLength(1);
});

test('unresolved patch cleanup prevents new comparisons and records independent deletion success', async ({ page }) => {
  const widget = await open(page, '?patchdeletealwaysfails');
  await widget.getByLabel('Before text', { exact: true }).fill('old');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('unused patch could not be deleted');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('Retry Clear before comparing again');
  expect((await calls(page)).filter(x => x.name === 'text.diff')).toHaveLength(1);
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByRole('alert')).toContainText('Clear has not completed');
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect.poll(async () => (await calls(page)).filter(x => x.name === 'text.attachment.delete').length).toBe(4);
  expect((await calls(page)).filter(x => x.name === 'text.diff.delete')).toHaveLength(1);
});

for (const suffix of ['badexplanation', 'badpatch', 'wrongpatchkind']) {
  test(`malformed ${suffix} responses clean independently validated handles`, async ({ page }) => {
    const widget = await open(page, `?${suffix}`);
    await widget.getByLabel('Before text', { exact: true }).fill('old');
    await widget.getByRole('button', { name: 'Compare', exact: true }).click();
    await expect(widget.getByRole('alert')).toContainText('unsupported comparison response');
    await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
    const invoked = await calls(page);
    expect(invoked.filter(x => x.name === 'text.diff.delete')).toHaveLength(1);
    expect(invoked.filter(x => x.name === 'text.attachment.delete')).toHaveLength(1);
    expect(invoked.some(x => x.name === 'text.diff.page')).toBe(false);
  });
}

test('cancellation during patch deletion cleans the comparison without exposing a late result', async ({ page }) => {
  const widget = await open(page, '?slowpatchdelete');
  await widget.getByLabel('Before text', { exact: true }).fill('old');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect.poll(async () => (await calls(page)).some(x => x.name === 'text.attachment.delete')).toBe(true);
  await page.evaluate(() => window.dispatchEvent(new Event('cancel-comparison')));
  await expect(widget.getByRole('alert')).toContainText('retained comparison was deleted');
  await expect(widget.getByRole('region', { name: 'Comparison result' })).toHaveCount(0);
  expect((await calls(page)).filter(x => x.name === 'text.diff.delete')).toHaveLength(1);
});

test('a stale successful comparison cleans both handles and preserves a newer host result', async ({ page }) => {
  const widget = await open(page);
  await widget.getByLabel('Before text', { exact: true }).fill('slow');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('status')).toHaveText('Comparing texts…');
  await page.evaluate(() => window.dispatchEvent(new Event('replace-comparison')));
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeEnabled();
  await expect(widget.locator('.diff-fragment')).toContainText('Replacement before');
  const invoked = await calls(page);
  expect(invoked.filter(x => x.name === 'text.attachment.delete')).toHaveLength(1);
  expect(invoked.filter(x => x.name === 'text.diff.delete')).toHaveLength(1);
  expect(invoked.find(x => x.name === 'text.diff.delete')?.arguments.comparison_id).not.toBe('f'.repeat(64));
});


test('a failed previous-result deletion survives replacement by a newer host input', async ({ page }) => {
  const widget = await open(page, '?pair&slowdeletefail');
  await expect(widget.locator('.diff-fragment')).toBeVisible();
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect.poll(async () => (await calls(page)).some(x => x.name === 'text.diff.delete')).toBe(true);
  await page.evaluate(() => window.dispatchEvent(new Event('replace-comparison')));
  await expect(widget.getByRole('button', { name: 'Clear', exact: true })).toBeEnabled();
  await expect(widget.locator('.diff-fragment')).toContainText('Replacement before');
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('');
  const deleted = (await calls(page)).filter(x => x.name === 'text.diff.delete');
  expect(deleted).toHaveLength(3);
  expect(deleted[1].arguments.comparison_id).toBe(deleted[0].arguments.comparison_id);
  expect(deleted[2].arguments.comparison_id).toBe('f'.repeat(64));
});


test('host replacement preserves cleanup ownership of the previously displayed comparison', async ({ page }) => {
  const widget = await open(page);
  await widget.getByLabel('Before text', { exact: true }).fill('old');
  await widget.getByRole('button', { name: 'Compare', exact: true }).click();
  await expect(widget.getByRole('button', { name: 'Compare', exact: true })).toBeEnabled();
  await page.evaluate(() => window.dispatchEvent(new Event('replace-comparison')));
  await expect(widget.locator('.diff-fragment')).toContainText('Replacement before');
  await widget.getByRole('button', { name: 'Clear', exact: true }).click();
  await expect(widget.getByLabel('Before text', { exact: true })).toHaveValue('');
  const deleted = (await calls(page)).filter(x => x.name === 'text.diff.delete');
  expect(deleted).toHaveLength(2);
  expect(deleted[0].arguments.comparison_id).not.toBe('f'.repeat(64));
  expect(deleted[1].arguments.comparison_id).toBe('f'.repeat(64));
});
