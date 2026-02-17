# Thrift Illustrated

Thrift Illustrated is a static learning site.
It shows Apache Thrift bytes from real local traffic.

The goal is simple: make Thrift wire data easy to see and easy to learn.
This project takes design ideas from [illustrated-tls12](https://tls12.xargs.org/).

![Thrift Illustrated screenshot](site/assets/screenshot.png)

## Why We Made This

Thrift tutorials often show only method calls and return values.
That is useful, but it hides the wire format.

This project helps you:

- See real bytes from real Ruby tutorial calls.
- Compare protocol and transport choices.
- Learn what each byte means.
- Review changes with stable, committed data files.

> [!IMPORTANT]
> Capture data must come from real localhost Ruby traffic. No fake wire fixtures.

## How It Works

The project has two parts.

1. A Ruby pipeline captures and parses traffic.
2. A static web app renders that data.

Pipeline flow:

1. Run tutorial client and server on localhost.
2. Record write chunks from client and server.
3. Parse bytes into messages, fields, and spans.
4. Validate schema and call-flow rules.
5. Write JSON artifacts into `site/data/captures/`.

UI flow:

1. Load `manifest.json`.
2. Load one combo dataset.
3. Check byte size and SHA-256.
4. Render timeline, summary, hexdump, and field tree.

Byte learning behavior:

- Hover a field to highlight its bytes.
- Hover a byte to highlight its field.
- Click a byte to show its byte group.
- Hover subfields to highlight exact sub-ranges.
- Hex and ASCII views stay linked.

> [!NOTE]
> The site is plain HTML, CSS, and JavaScript. There is no bundling step.

## Supported Combinations

```text
binary + buffered
binary + framed
compact + buffered
compact + framed
json + buffered
json + framed
header + header
```

## Quick Start

Prerequisites:

- `mise`
- Ruby `4.0.1`
- Bun `1.3.5`

Setup:

```bash
mise install
mise run deps
```

Capture data:

```bash
mise run gen
mise exec -- bundle exec rake capture
```

Run Ruby checks:

```bash
mise exec -- bundle exec rake check
```

Run cross-language checks (Ruby + JavaScript + benchmark schema):

```bash
mise run ci
```

Run local web server:

```bash
mise run web
```

Open [http://127.0.0.1:8000/](http://127.0.0.1:8000/).

## Common Options

Capture command:

```bash
bundle exec ruby scripts/capture_all.rb [--combo <id>] [--output <dir>] [--strict]
```

Flags:

- `--combo`: run only one combo.
- `--output`: write artifacts to another repo-local path.
- `--strict`: stop on first non-warning parse error.

Env var:

- `THRIFT_TIMEOUT_MS` (default `5000`)

UI hash state:

```text
#combo=<combo-id>&msg=<index>
```

Example:

```text
#combo=compact-framed&msg=4
```

## CI and Dependency Updates

CI runs on push to `main` and on pull requests.

CI commands:

```bash
mise run ci
```

Dependabot updates:

- Bundler gems
- GitHub Actions
- npm/Bun packages

## Contributing

1. Read `CONSTITUTION.md` first.
2. Keep the site static.
3. Keep captures real and local.
4. If parser or capture code changes, regenerate artifacts.
5. Run tests before opening a PR.

Useful commands:

```bash
mise exec -- bundle exec rake check
mise run ci
mise exec -- bun run scripts/ui_smoke_test.mjs
```

If browser checks fail, set `CHROME_BIN` to your Chrome or Chromium path.

## Project Layout

```text
site/        static UI, capture data, and schemas
scripts/     capture, parser, validation, benchmark, and health scripts
thrift/      tutorial IDL and generated Ruby stubs (`gen-rb`)
test/        Ruby unit and integration tests
mise.toml    pinned tools and local tasks (`web`, `gen`, `ci`)
```
