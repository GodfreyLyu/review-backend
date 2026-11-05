# Instructions for this project

## Commands for the user to copy

- Keep each shell command on one physical line. Do not insert hard line breaks,
  backslash continuations, or heredocs that require multiline pasting.
- Put each command in its own unindented fenced code block. Include only the command:
  no shell prompts, line numbers, list markers, comments, or sample output. Keep
  explanations outside the block.
- Use ASCII quotes, hyphens, and spaces for shell syntax. Do not introduce smart
  quotes, non-breaking spaces, zero-width characters, or decorative punctuation.
  Preserve legitimate non-ASCII characters in paths and argument values.
- Prefer short commands. When a command needs complex quoting, multiline logic, or
  excessive length, save the logic as a workspace script and provide a short,
  single-line invocation. Preserve shell semantics rather than blindly joining lines.
- When the user explicitly requests "command only" or "只给命令", output just the
  requested single-line command as plain text, without Markdown fences or explanation,
  so Codex CLI's `/copy` can copy it directly.
- These rules apply to commands presented for the user to copy and execute. Source
  code, configuration files, and internal tool scripts should retain their appropriate
  formatting.
