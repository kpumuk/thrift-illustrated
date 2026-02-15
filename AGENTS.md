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

## Commands

- `mise install`
- `bundle install`
- `bundle exec rake capture`
- `bundle exec rake test`
- `bundle exec rake build`
- `bundle exec rake check`

## Rules

- Capture bytes from real localhost Ruby Thrift client/server traffic (no synthetic fixtures).
- Commit generated artifacts in `thrift/gen-rb` and `data/captures`.
- If tutorial flow, parser behavior, or combo matrix changes, regenerate captures and update tests in the same change.
