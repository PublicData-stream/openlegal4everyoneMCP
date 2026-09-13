// Check the installed, frozen-lockfile graph rather than only direct dependencies.
import { readdir, readFile, lstat } from 'node:fs/promises';
import { join } from 'node:path';
const allowed = new Set(['MIT', 'Apache-2.0', 'ISC']);
// Exact additions admitted for the comparison renderer; the distributed HTML
// includes their original copyright, BSD terms and disclaimer. Owner: PiQuark6046;
// review by 2026-12-12 or version change. See README.
const reviewedBsd = new Set(['highlight.js@11.11.2', 'highlight.js@11.12.0']);
const reviewed = new Set();
for (const entry of await readdir('node_modules/.pnpm')) {
  const base = join('node_modules/.pnpm', entry, 'node_modules');
  let children;
  try { children = await readdir(base); } catch { continue; }
  for (const child of children) {
    const paths = child.startsWith('@') ? (await readdir(join(base, child))).map(name => join(base, child, name)) : [join(base, child)];
    for (const path of paths) {
      if (!(await lstat(path)).isDirectory()) continue;
      const pkg = JSON.parse(await readFile(join(path, 'package.json'), 'utf8'));
      if (!allowed.has(pkg.license) && !(pkg.license === 'BSD-3-Clause' && reviewedBsd.has(`${pkg.name}@${pkg.version}`))) throw new Error(`Review required for ${pkg.name}: ${pkg.license}`);
      reviewed.add(`${pkg.name}@${pkg.version}`);
    }
  }
}
if (reviewed.size === 0) throw new Error('No installed dependency manifests were checked');
console.log(`Checked ${reviewed.size} installed dependency licenses (MIT, Apache-2.0, ISC; exact reviewed highlight.js BSD-3-Clause versions)`);
