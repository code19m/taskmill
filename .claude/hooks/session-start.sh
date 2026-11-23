#!/bin/sh
# Starts Docker and the test database when a Claude Code cloud session starts,
# so that `make check` runs there as it does in CI. Cloud containers have
# Docker installed, but its daemon is not running. Anywhere else this does
# nothing: developers run `make db` themselves (CONTRIBUTING.md).
set -eu

[ "${CLAUDE_CODE_REMOTE:-}" = true ] || exit 0

if ! docker info >/dev/null 2>&1; then
	if ! command -v dockerd >/dev/null; then
		echo "Docker is not installed, so the tests have no database." >&2
		exit 1
	fi
	# A session of its own, so that the daemon outlives this script.
	setsid dockerd </dev/null >/var/log/dockerd.log 2>&1 &
	tries=0
	until docker info >/dev/null 2>&1; do
		tries=$((tries + 1))
		if [ "$tries" -gt 150 ]; then # a daemon that failed to start never answers
			echo "Docker did not start in 30 seconds. The end of /var/log/dockerd.log:" >&2
			tail -n 20 /var/log/dockerd.log >&2
			exit 1
		fi
		sleep 0.2
	done
fi

cd "$CLAUDE_PROJECT_DIR"
make db
