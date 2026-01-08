# Codex context – casalib

You are helping review and refactor the casalib codebase.

Goals:
- Make this repository professional
- Reduce overengineering
- Flatten abstractions where possible
- Keep strong typing (mypy-first)
- Keep extensibility for multiple SQL backends
- Prefer Protocols over deep ABC hierarchies

Known issues:
- Too many abstraction layers in data_connection
- Connection objects forward to sub-objects excessively
- TableManager + helpers feel overdesigned
- Some duplicated modules (e.g., data_paths)

Rules:
- Do not refactor everything at once
- Propose incremental changes
- Keep public imports stable unless explicitly told otherwise