# AGENTS

## First Instruction (Read Before Anything Else)

- Always consult `CONSTITUTION.md` first.
- Treat the constitution as the project's source of truth for defaults and constraints.
- If any instruction conflicts with the constitution, follow the constitution.

## Scope

- Static site illustrating Apache Thrift Ruby tutorial traffic.
- Supported combinations:
  - `binary + buffered`
  - `binary + framed`
  - `compact + buffered`
  - `compact + framed`
  - `json + buffered`
  - `json + framed`
  - `header + header`

## Commands

- `mise install`
- `mise run deps`
- `bundle install`
- `bun install`
- `bundle exec rake capture`
- `bundle exec rake lint`
- `bundle exec rake test`
- `bundle exec rake schema`
- `bundle exec rake check`
- `mise run ci`

## Rules

- Capture bytes from real localhost Ruby Thrift client/server traffic (no synthetic fixtures).
- Commit generated artifacts in `thrift/gen-rb` and `data/captures`.
- If tutorial flow, parser behavior, or combo matrix changes, regenerate captures and update tests in the same change.
- Treat byte-level pedagogy as a hard requirement: every wire byte must map to a meaningful explanation group, and envelope/header groups must expose hoverable subfield breakdowns with exact byte spans.
