# Project conventions

## Git conventions

All commits must be signed and signed-off. Use `git commit -S -s`.

## Task runner

This project uses [mise](https://mise.jq.sh/) for running tasks. Check `mise.toml` for available tasks (`mise tasks`). If you find yourself running a common task ad-hoc repeatedly, add it to `mise.toml` instead of relying on manual commands.

