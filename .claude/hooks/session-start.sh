#!/bin/sh
# Readies a Claude Code cloud session for `make check`, as CONTRIBUTING.md
# readies a machine. The Go that go.mod selects goes first on PATH, because the
# Makefile builds its linters with the Go on PATH, and the image's may be
# older. Docker, installed but not running, starts with the test database.
# Anywhere else this does nothing.
set -eu
[ "${CLAUDE_CODE_REMOTE:-}" = true ] || exit 0
cd "$CLAUDE_PROJECT_DIR"

printf 'export PATH="%s/bin:$PATH"\n' "$(go env GOROOT)" >> "$CLAUDE_ENV_FILE"

if ! docker info >/dev/null 2>&1; then
	setsid dockerd </dev/null >/var/log/dockerd.log 2>&1 &
	timeout 30 sh -c 'until docker info >/dev/null 2>&1; do sleep 0.2; done' ||
		{ echo 'Docker did not start: see /var/log/dockerd.log' >&2; exit 1; }
fi
make db
