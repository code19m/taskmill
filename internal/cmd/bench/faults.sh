#!/bin/sh
# Runs the fault scenarios at the size CI runs them nightly, against
# databases of their own, and removes those databases afterwards. It fails on
# any violation. `make faults` runs it; it needs Docker and Go.
#
# - F1 to F7: the benchmark kills workers and producers, restarts the
#   database, and cuts a worker off from it.
# - F9: the timing scenario runs against copies of the database whose clocks
#   are 5 minutes ahead and 5 minutes behind this machine's.
#
# F8, the tests through PgBouncer, is `make test-pgbouncer`, which CI runs on
# every pull request.
set -eu

postgres=postgres:18-alpine@sha256:10a5c69a83035ab5239f5620d708520e8b28f5aa1f2b20143e6024532edadfb5
faketime=libfaketime=0.9.13-r0
db=taskmill-faults-db
url() { echo "postgres://postgres:postgres@localhost:$1/taskmill?sslmode=disable"; }

cleanup() {
	status=$?
	if [ "$status" -ne 0 ]; then
		for c in $db $db-ahead $db-behind; do
			echo "--- last log lines of $c" >&2
			docker logs --tail 30 "$c" >&2 2>&1 || true
		done
	fi
	docker rm -f -v $db $db-ahead $db-behind >/dev/null 2>&1 || true
	exit "$status"
}
trap cleanup EXIT INT TERM

docker run -d --name $db -p 54340:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
	"$postgres" -c max_connections=500 >/dev/null
# libfaketime offsets the database's clock; the application keeps the real one.
printf 'FROM %s\nRUN apk add --no-cache %s\n' "$postgres" "$faketime" | docker build -q -t taskmill-faults-faketime - >/dev/null
for skew in ahead:+5m:54341 behind:-5m:54342; do
	docker run -d --name "$db-${skew%%:*}" -p "${skew##*:}:5432" -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
		-e LD_PRELOAD=/usr/lib/faketime/libfaketime.so.1 -e FAKETIME="$(echo "$skew" | cut -d: -f2)" \
		-e DONT_FAKE_MONOTONIC=1 taskmill-faults-faketime >/dev/null
done
for c in $db $db-ahead $db-behind; do
	until docker exec "$c" pg_isready -h 127.0.0.1 -U postgres -q; do sleep 0.2; done
done

go build -o bin/bench ./internal/cmd/bench
bin/bench -db "$(url 54340)" -short -container $db -scenarios chaos,order,dedupe,restart,partition
bin/bench -db "$(url 54341)" -skew 5m -scenarios timing
bin/bench -db "$(url 54342)" -skew -5m -scenarios timing
