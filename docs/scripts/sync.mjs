// Copies files kept at the repository root (CHANGELOG.md, MSRV.md, the plugin's CHANGELOG.md)
// into content/ (all gitignored), adds frontmatter and points links to other repository files
// at GitHub.
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, posix } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = new URL('../../', import.meta.url)
const content = new URL('../content/', import.meta.url)
const repo = 'https://github.com/s00d/rust-socketio/blob/main/'

// Repository files that are pages of the site.
const sitePaths = {
  'MSRV.md': '/getting-started/msrv',
  'CHANGELOG.md': '/changelog',
  'crates/tauri-plugin-socketio/CHANGELOG.md': '/tauri/changelog',
}

const pages = [
  {
    from: 'CHANGELOG.md',
    to: '9.changelog.md',
    frontmatter: { title: 'Changelog', description: 'All notable changes to socketio-next.' },
  },
  {
    from: 'MSRV.md',
    to: '1.getting-started/9.msrv.md',
    frontmatter: { title: 'MSRV', description: 'Minimum supported Rust version and how it is checked.' },
  },
  {
    from: 'crates/tauri-plugin-socketio/CHANGELOG.md',
    to: '3.tauri/9.changelog.md',
    frontmatter: { title: 'Plugin changelog', description: 'All notable changes to tauri-plugin-socketio and tauri-plugin-socketio-api.' },
  },
]

for (const page of pages) {
  let body = readFileSync(new URL(page.from, root), 'utf8')
  // The page title comes from frontmatter; drop the file's own `# Heading`.
  body = body.replace(/^# .*\n/, '')
  // Relative links would 404 on the site: resolve them against the source file.
  body = body.replace(/\]\((?!https?:|#|\/)([^)]+)\)/g, (_, target) => {
    const path = posix.normalize(posix.join(posix.dirname(page.from), target))
    return `](${sitePaths[path] ?? repo + path})`
  })
  const fm = Object.entries(page.frontmatter)
    .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
    .join('\n')
  const out = new URL(page.to, content)
  mkdirSync(dirname(fileURLToPath(out)), { recursive: true })
  writeFileSync(out, `---\n${fm}\n---\n\n<!-- Generated from ${page.from} by docs/scripts/sync.mjs. Edit the original. -->\n\n${body}`)
  console.log(`synced ${page.from} -> ${fileURLToPath(out)}`)
}
