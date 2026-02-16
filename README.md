# Thrift Illustrated

## Bootstrap

1. Install toolchain defined in `mise.toml`:
   - `mise install`
2. Install Ruby gems in the pinned Ruby environment:
   - `mise exec -- bundle install`
3. Install Bun dependencies (UI benchmark harness):
   - `bun install`
4. List baseline tasks:
   - `mise exec -- bundle exec rake -T`

## Baseline Tasks

- `bundle exec rake capture`
- `bundle exec rake test`
- `bundle exec rake build`
- `bundle exec rake check`
- `mise run web` (then open `http://127.0.0.1:8000/`)

## Deployment Ops

- Browser health probe script: `scripts/health_check_browser.mjs`.
