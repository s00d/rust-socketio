# Development and CI tasks. `just` lists them; `just ci` is what the CI workflow runs.
# Tools: `cargo install just cargo-hack cargo-deny cargo-shear cargo-sweep`; Node 20+ for the
# test servers (`just servers`).
#
# Builds use the `ci` profile (no debuginfo, output in target/ci). Every build is `--locked`.

set shell := ["bash", "-euo", "pipefail", "-c"]

ci_flags := "--profile ci --locked"
crate := "socketio-next"

[private]
default:
    @just --list

# Everything CI runs (the servers must be up: `just servers`)
ci: fmt lint test docs deny package

# rustfmt check
fmt:
    cargo fmt --all --check

# Unused dependencies, then clippy -D warnings on the workspace and on every feature set
lint:
    cargo shear
    cargo clippy --workspace --all-targets --all-features {{ ci_flags }} -- -D warnings
    cargo hack clippy -p {{ crate }} --feature-powerset --all-targets {{ ci_flags }} -- -D warnings

# All tests and doctests, then the crate per feature set (no TLS, rustls, native-tls, blocking);
# needs the Node servers (`just servers`)
test:
    cargo test --workspace --all-targets --all-features {{ ci_flags }}
    cargo test --workspace --doc --all-features {{ ci_flags }}
    cargo hack test -p {{ crate }} --each-feature --all-targets {{ ci_flags }}

# Tests that need no server: the codecs, backoff, redaction, error texts
test-offline:
    cargo test -p {{ crate }} --lib {{ ci_flags }} -- protocol:: backoff:: redact:: util:: tls:: actor::tests

# Install the Node test servers' packages and generate the test certificates
servers-setup:
    cd ci && npm ci --no-audit --no-fund
    cd ci/conformance && npm ci --no-audit --no-fund
    ./ci/keygen.sh node-engine-io-secure 127.0.0.1

# Start the Node test servers in the background (ports 4200-4206, 4210-4211)
servers:
    ./ci/servers.sh start

# Stop them
servers-stop:
    ./ci/servers.sh stop

# rustdoc with warnings denied (docs.rs adds `--cfg docsrs` on nightly for the feature badges)
docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features {{ ci_flags }}

# Advisories, licenses, duplicate and banned crates (deny.toml)
deny:
    cargo deny --workspace --all-features check

# What `cargo publish` would upload, verified by a build; fails when test fixtures slip in
package *args:
    cargo package -p {{ crate }} --locked {{ args }}
    @if cargo package -p {{ crate }} --list --locked --allow-dirty | grep -E '^(ci|tests|benches|examples)/' ; then echo "test files in the package" >&2; exit 1; fi

# The crate on its rust-version with a lockfile resolved for it (MSRV.md; needs that toolchain)
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    tc="$(cargo metadata --no-deps --format-version 1 --locked | python3 -c 'import json,sys; print(next(p["rust_version"] for p in json.load(sys.stdin)["packages"] if p["name"]=="{{ crate }}"))')"
    dir="$(mktemp -d)"; trap 'rm -rf "${dir}"' EXIT
    mkdir "${dir}/src"; echo 'fn main() {}' > "${dir}/src/main.rs"
    for features in '"rustls"' '"native-tls", "blocking"'; do
      printf '[package]\nname = "msrv-probe"\nversion = "0.0.0"\nedition = "2024"\nrust-version = "%s"\npublish = false\n[dependencies]\n{{ crate }} = { path = "%s", default-features = false, features = [%s] }\n[workspace]\n' \
        "${tc}" "${PWD}/crates/{{ crate }}" "${features}" > "${dir}/Cargo.toml"
      echo "==> MSRV ${tc}: [${features}]"
      (cd "${dir}" && rm -f Cargo.lock && cargo generate-lockfile -q \
        && CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}/msrv" cargo "+${tc}" check -q --locked)
    done

# Run the examples against a local server (see examples/README)
example name *args:
    cargo run -p examples --bin {{ name }} {{ ci_flags }} -- {{ args }}

# Cap target/ at 10G, oldest artifacts first (needs cargo-sweep)
trim:
    @if command -v cargo-sweep >/dev/null; then cargo sweep --maxsize 10GB .; else echo "cargo-sweep not installed: target/ not trimmed"; fi

# Remove all build output
clean:
    cargo clean

# cargo-release pre-release hook (after the version bump): checks, then
# [Unreleased] -> [version] in CHANGELOG.md
[private]
release-hook version:
    just fmt lint docs
    just package --allow-dirty
    @if [[ "${DRY_RUN:-false}" == "true" ]]; then echo "dry run: CHANGELOG.md unchanged"; else just _changelog {{ version }}; fi

[private]
_changelog version:
    #!/usr/bin/env python3
    import datetime, re, sys
    from pathlib import Path
    path, version = Path("CHANGELOG.md"), "{{ version }}"
    text = path.read_text()
    if re.search(rf"^## \[{re.escape(version)}\]", text, re.M):
        sys.exit(0)
    m = re.search(r"^## \[Unreleased\][^\n]*\n", text, re.M)
    rest = text[m.end():] if m else ""
    nxt = re.search(r"^## \[", rest, re.M)
    if not m or not rest[: nxt.start() if nxt else None].strip():
        sys.exit("CHANGELOG.md: write the release notes under '## [Unreleased]' first")
    path.write_text(text[: m.start()] + f"## [Unreleased]\n\n## [{version}] - {datetime.date.today()}\n" + rest)
    print(f"CHANGELOG.md: [Unreleased] -> [{version}]")
