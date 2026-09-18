# Project conventions

## Git conventions

All commits must be signed and signed-off. Use `git commit -S -s`.

## Task runner

This project uses [mise](https://mise.jq.sh/) for running tasks. Check `mise.toml` for available tasks (`mise tasks`). If you find yourself running a common task ad-hoc repeatedly, add it to `mise.toml` instead of relying on manual commands.

## Code comments

This project values good, readable, and easy-to-understand code over excessive comments. Do not add comments for obvious things. Think twice before adding a comment and only add one if it is truly worth it and the intent or mechanism cannot be figured out or inferred directly by reading the code.
