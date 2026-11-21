# Publishing

`socketio-next` and `tauri-plugin-socketio` (crates) and the npm package
`tauri-plugin-socketio-api` share one version and are released from GitHub Actions. Nothing is
published from a laptop and no crates.io or npm token is stored anywhere.

## Release

1. Keep `## [Unreleased]` in `CHANGELOG.md` (and in `crates/tauri-plugin-socketio/CHANGELOG.md`
   when the plugin changed) filled in by hand. It is never generated from git.
2. Wait for green CI on `main`.
3. Actions -> **CI** -> **Run workflow** -> `release`: the version (e.g. `0.2.0`, no `v`) -> Run.
   The `release` job:
   - stops unless CI is green on `main` HEAD, or on an earlier commit with only docs, `*.md`
     files or the justfile changed since;
   - `just release-prep X.Y.Z`: sets the version of both crates and of the npm package, updates
     `Cargo.lock`, renames `[Unreleased]` to `[X.Y.Z] - date` in both changelogs (stops when the
     root one is empty; an empty plugin section gets "Built on socketio-next X.Y.Z");
   - `just semver`: compares the public API with crates.io and stops when the change is bigger
     than the version allows;
   - commits `chore: release vX.Y.Z` to `main`, tags `socketio-next-vX.Y.Z` and
     `tauri-plugin-socketio-vX.Y.Z`;
   - publishes `socketio-next`, then `tauri-plugin-socketio`, with crates.io trusted publishing,
     and `tauri-plugin-socketio-api` with npm trusted publishing;
   - creates a GitHub release per tag from its changelog section (`just release-notes`);
   - then the `docs` workflow (it runs after a successful manual CI run) rebuilds
     https://s00d.github.io/rust-socketio/.

   If it fails after tagging (network, crates.io, npm), run it again with the same version: it
   publishes only what is missing and creates the GitHub releases that are missing.

Run workflow with an empty `release` field runs the extended checks instead (semver against
crates.io, the example Tauri app, latest stable clippy).

The tags are per crate (`<crate>-vX.Y.Z`): plain `vX.Y.Z` tags belong to the rust_socketio fork
history in this repository.

## Setup (once)

1. **crates.io**, for each crate: [socketio-next settings](https://crates.io/crates/socketio-next/settings)
   and [tauri-plugin-socketio settings](https://crates.io/crates/tauri-plugin-socketio/settings) ->
   Trusted Publishing -> Add -> GitHub: owner `s00d`, repository `rust-socketio`, workflow
   `ci.yml`, environment `crates-io`.
2. **npm**: [tauri-plugin-socketio-api settings](https://www.npmjs.com/package/tauri-plugin-socketio-api/access)
   -> Trusted Publisher -> GitHub Actions: organization or user `s00d`, repository
   `rust-socketio`, workflow filename `ci.yml`, environment `crates-io`. Optionally, below it,
   "Require two-factor authentication and disallow tokens" so only CI can publish.
3. **GitHub**: [Environments](https://github.com/s00d/rust-socketio/settings/environments) ->
   New environment `crates-io` (GitHub creates it on the first run anyway); add yourself as a
   required reviewer to approve every release run. Workflow permissions stay read-only by
   default ([Actions settings](https://github.com/s00d/rust-socketio/settings/actions)): the
   `release` job asks for `contents: write` and `id-token: write` itself. `main` must accept a
   push from `github-actions[bot]` (it is not protected now; with a ruleset, allow the bot).

## crates.io trusted publishing

Each crate has a trusted publisher on crates.io (crate page -> Settings -> Trusted Publishing ->
Add -> GitHub): owner `s00d`, repository `rust-socketio`, workflow `ci.yml`, environment
`crates-io`. The environment must match the `environment:` of the `release` job, otherwise the
token exchange is refused (the same holds for npm). GitHub creates the `crates-io` environment on the first run; to limit
who can release, add required reviewers to it (repository Settings -> Environments).

After the first release through CI, turn on **Trusted Publishing only** on each crate's settings
page: token publishes are refused from then on, and the old API tokens can be deleted.

crates.io cannot create a crate through trusted publishing: a new crate is published by hand
once (a `0.0.0` placeholder is enough), then gets its trusted publisher.
