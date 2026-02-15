# Thrift Illustrated

## Bootstrap

1. Install toolchain defined in `mise.toml`:
   - `mise install`
2. Install Ruby gems in the pinned Ruby environment:
   - `mise exec -- bundle install`
3. List baseline tasks:
   - `mise exec -- bundle exec rake -T`

## Baseline Tasks

- `bundle exec rake capture`
- `bundle exec rake test`
- `bundle exec rake build`
- `bundle exec rake check`

These are skeleton tasks during bootstrap and are expected to be replaced with full implementations.
