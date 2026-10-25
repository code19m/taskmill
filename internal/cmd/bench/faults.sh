#!/bin/sh
# Runs the fault scenarios, F1 to F9, at the size CI runs them, against
# databases of their own, and removes those databases afterwards. It fails on
# any violation. `make faults` runs it; it needs Docker and Go.
#
# - F1 to F7: the benchmark kills workers and producers, restarts the
#   database, and cuts a worker off from it.
# - F8: the tests run through PgBouncer in transaction mode, with LISTEN on a
#   direct connection.
# - F9: the timing scenario runs against copies of the database whose clocks
#   are 5 minutes ahead and 5 minutes behind this machine's.
set -eu

postgres=${FAULTS_POSTGRES:-postgres:18-alpine}
pgbouncer=edoburu/pgbouncer:v1.25.2-p0@sha256:7d7a27d9e90985cab5cf42256f5c13a3120baa4b055b69df37beb272b89b2340
net=taskmill-faults
db=taskmill-faults-db
url() { echo "postgres://postgres:postgres@localhost:$1/taskmill?sslmode=disable"; }

cleanup() {
	status=$?
	if [ "$status" -ne 0 ]; then
		for c in $db $db-ahead $db-behind $db-pgbouncer; do
			echo "--- last log lines of $c" >&2
			docker logs --tail 30 "$c" >&2 2>&1 || true
		done
	fi
	docker rm -f -v $db $db-ahead $db-behind $db-pgbouncer >/dev/null 2>&1 || true
	docker network rm $net >/dev/null 2>&1 || true
	exit "$status"
}
trap cleanup EXIT INT TERM

docker network create $net >/dev/null
docker run -d --name $db --network $net -p 54340:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
	"$postgres" -c max_connections=500 >/dev/null
# libfaketime offsets the database's clock; the application keeps the real one.
printf 'FROM %s\nRUN apk add --no-cache libfaketime\n' "$postgres" | docker build -q -t taskmill-faults-faketime - >/dev/null
for skew in ahead:+5m:54341 behind:-5m:54342; do
	docker run -d --name "$db-${skew%%:*}" -p "${skew##*:}:5432" -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=taskmill \
		-e LD_PRELOAD=/usr/lib/faketime/libfaketime.so.1 -e FAKETIME="$(echo "$skew" | cut -d: -f2)" \
		-e DONT_FAKE_MONOTONIC=1 taskmill-faults-faketime >/dev/null
done
docker run -d --name $db-pgbouncer --network $net -p 54343:5432 -e DB_HOST=$db -e DB_NAME=taskmill \
	-e DB_USER=postgres -e DB_PASSWORD=postgres -e AUTH_TYPE=scram-sha-256 -e POOL_MODE=transaction \
	-e MAX_CLIENT_CONN=1000 -e DEFAULT_POOL_SIZE=40 -e MAX_PREPARED_STATEMENTS=1000 "$pgbouncer" >/dev/null
for c in $db $db-ahead $db-behind; do
	until docker exec "$c" pg_isready -h 127.0.0.1 -U postgres -q; do sleep 0.2; done
done

go build -o bin/bench ./internal/cmd/bench
bin/bench -db "$(url 54340)" -short -container $db -scenarios chaos,order,dedupe,restart,partition
TASKMILL_TEST_DATABASE_URL="$(url 54343)" TASKMILL_TEST_LISTEN_URL="$(url 54340)" go test -race -count=1 ./...
bin/bench -db "$(url 54341)" -scenarios timing
bin/bench -db "$(url 54342)" -scenarios timing
