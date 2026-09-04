#!/usr/bin/env bash
# SOT: all-engine-live-test-runner, docker-compose-test-driver
#
# WHAT:  Brings up docker-compose.test.yml, waits for every engine to answer,
#        runs that engine's live round-trip test with the right environment,
#        and prints one line per engine: PASS / FAIL / SKIP (+ why).
# WHY:   "the adapters work" is only true if a real server says so. This is the
#        acceptance check for the engine catalogue.
# HOW:   ./scripts/test-all-dbs.sh                # every engine in the stack
#        ./scripts/test-all-dbs.sh qdrant neo4j   # just these
#        KEEP_UP=1 ./scripts/test-all-dbs.sh      # leave containers running
#        WITH_HEAVY=1 ./scripts/test-all-dbs.sh   # include oracle + druid
# WHERE: docker-compose.test.yml (the stack), scripts/live-tests.sh (one-off)
set -uo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
COMPOSE=(docker compose -f docker-compose.test.yml)
[ "${WITH_HEAVY:-0}" = "1" ] && COMPOSE+=(--profile heavy)

PASS=(); FAIL=(); SKIP=()

log()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }

# Engines whose test needs no server (file-based) still run, so a full pass
# covers them too.
FILE_ENGINES=(sqlite duckdb rocksdb)

# service:engine[:extra-service…] — the compose services an engine test needs.
SERVICES=(
  postgres mysql mariadb mssql
  mongodb couchdb
  redis valkey memcached dynamodb
  cassandra hbase
  neo4j
  influxdb prometheus victoriametrics
  qdrant chroma weaviate milvus
  elasticsearch opensearch meilisearch typesense
  arangodb surrealdb orientdb
  clickhouse
  immudb
  redpanda
  existdb
  fuseki
)
[ "${WITH_HEAVY:-0}" = "1" ] && SERVICES+=(oracle druid)

SELECTED=("$@")
wanted() {
  [ ${#SELECTED[@]} -eq 0 ] && return 0
  for s in "${SELECTED[@]}"; do [ "$s" = "$1" ] && return 0; done
  return 1
}

# WHAT:  Runs one engine's live test with its environment already exported.
#        The test itself is a no-op when its gate variable is unset, so a
#        missing service degrades to SKIP rather than a false failure.
run_engine() { # run_engine <name> <cargo filter> <env assignments…>
  local name="$1" filter="$2"; shift 2
  log "$name"
  local out
  if ! out=$(cd "$ROOT/src-tauri" && env "$@" cargo test --lib "$filter" -- --test-threads=1 2>&1); then
    printf '%s\n' "$out" | grep -E "panicked at|assertion|error\[|^error" | head -5
    FAIL+=("$name")
    return 1
  fi
  # `0 passed` means the gate variable was unset: the server never came up.
  if printf '%s\n' "$out" | grep -qE "test result: ok\. 0 passed"; then
    note "skipped (server not reachable)"
    SKIP+=("$name")
    return 0
  fi
  printf '%s\n' "$out" | grep -E "^test result" | head -1 | sed 's/^/   /'
  PASS+=("$name")
}

# WHAT:  The published host port for each service, so readiness can be probed
#        from here rather than from inside images that may lack curl or a shell.
port_of() {
  case "$1" in
    postgres) echo 55432 ;; mysql) echo 53306 ;; mariadb) echo 53307 ;; mssql) echo 51433 ;;
    mongodb) echo 57017 ;; couchdb) echo 55984 ;;
    redis) echo 56379 ;; valkey) echo 56380 ;; memcached) echo 51211 ;; dynamodb) echo 58000 ;;
    cassandra) echo 59042 ;; hbase) echo 58080 ;;
    neo4j) echo 57687 ;;
    influxdb) echo 58086 ;; prometheus) echo 59090 ;; victoriametrics) echo 58428 ;;
    qdrant) echo 56333 ;; chroma) echo 58001 ;; weaviate) echo 58081 ;; milvus) echo 59091 ;;
    elasticsearch) echo 59200 ;; opensearch) echo 59201 ;; meilisearch) echo 57700 ;; typesense) echo 58108 ;;
    arangodb) echo 58529 ;; surrealdb) echo 58002 ;; orientdb) echo 52480 ;;
    clickhouse) echo 58123 ;; druid) echo 58888 ;;
    immudb) echo 58082 ;;
    redpanda) echo 59092 ;;
    existdb) echo 58083 ;; fuseki) echo 53030 ;;
    oracle) echo 51521 ;;
    *) echo "" ;;
  esac
}

# WHAT:  True once the service accepts a TCP connection on its published port
#        (and, for the few that need warm-up, answers a real request).
# WHY:   Image-agnostic: no assumption about curl, wget or a shell inside the
#        container, which is exactly what made in-container healthchecks flaky.
ready() { # ready <service> <seconds>
  local svc="$1" secs="${2:-180}" port
  port=$(port_of "$svc")
  [ -z "$port" ] && return 1
  local id
  id=$("${COMPOSE[@]}" ps -q "$svc" 2>/dev/null)
  [ -z "$id" ] && return 1
  for _ in $(seq "$secs"); do
    if [ "$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null)" != "running" ]; then
      sleep 1
      continue
    fi
    if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then
      case "$svc" in
        # These accept TCP well before they can answer a query.
        cassandra) docker exec "$id" cqlsh -e 'SELECT release_version FROM system.local' >/dev/null 2>&1 && return 0 ;;
        mssql) curl -s --max-time 2 "http://127.0.0.1:$port" >/dev/null 2>&1; return 0 ;;
        mysql|mariadb) docker exec "$id" sh -c 'mariadb -h 127.0.0.1 -uroot -pdbfree -e "SELECT 1" 2>/dev/null || mysql -h 127.0.0.1 -uroot -pdbfree -e "SELECT 1" 2>/dev/null' >/dev/null 2>&1 && return 0 ;;
        postgres) docker exec "$id" pg_isready -U postgres -d dbfree >/dev/null 2>&1 && return 0 ;;
        *) return 0 ;;
      esac
    fi
    sleep 1
  done
  return 1
}

cleanup() {
  if [ "${KEEP_UP:-0}" = "1" ]; then
    note "KEEP_UP=1 — leaving the stack running (\`docker compose -f docker-compose.test.yml down\` to stop)."
  else
    log "tearing down"
    "${COMPOSE[@]}" down --remove-orphans --timeout 5 >/dev/null 2>&1
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- start
TO_START=()
for s in "${SERVICES[@]}"; do wanted "$s" && TO_START+=("$s"); done
# Milvus needs its own dependencies pulled in.
for s in "${TO_START[@]}"; do
  [ "$s" = "milvus" ] && TO_START+=(milvus-etcd milvus-minio)
done

if [ ${#TO_START[@]} -eq 0 ] && [ ${#SELECTED[@]} -gt 0 ]; then
  echo "No known service matches: ${SELECTED[*]}" >&2
  exit 2
fi

# Booting 30 engines at once starves a laptop and everything times out at
# the same moment; BATCH keeps the machine responsive.
BATCH="${BATCH:-8}"
log "starting ${#TO_START[@]} service(s) in batches of $BATCH"
i=0
while [ $i -lt ${#TO_START[@]} ]; do
  chunk=("${TO_START[@]:$i:$BATCH}")
  note "up: ${chunk[*]}"
  "${COMPOSE[@]}" up -d --no-recreate "${chunk[@]}" >/dev/null 2>&1
  for svc in "${chunk[@]}"; do
    if ready "$svc" 240; then note "  $svc ready"; else note "  $svc NOT ready"; fi
  done
  i=$((i + BATCH))
done

# ---------------------------------------------------------------- test
# Engines with no server dependency.
for e in "${FILE_ENGINES[@]}"; do
  wanted "$e" || [ ${#SELECTED[@]} -eq 0 ] || continue
  run_engine "$e" "integrations::${e}::"
done

wanted postgres && ready postgres 5 && run_engine postgres integrations::postgres::tests::live \
  DB_FREE_PG_HOST=127.0.0.1 DB_FREE_PG_PORT=55432 DB_FREE_PG_USER=postgres DB_FREE_PG_PASSWORD=dbfree DB_FREE_PG_DB=dbfree

wanted mysql && ready mysql 5 && run_engine mysql integrations::mysql::tests::live \
  DB_FREE_MYSQL_HOST=127.0.0.1 DB_FREE_MYSQL_PORT=53306 DB_FREE_MYSQL_USER=root DB_FREE_MYSQL_PASSWORD=dbfree DB_FREE_MYSQL_DB=dbfree

wanted mariadb && ready mariadb 5 && run_engine mariadb integrations::mysql::tests::live \
  DB_FREE_MYSQL_HOST=127.0.0.1 DB_FREE_MYSQL_PORT=53307 DB_FREE_MYSQL_USER=root DB_FREE_MYSQL_PASSWORD=dbfree DB_FREE_MYSQL_DB=dbfree

wanted mssql && ready mssql 5 && run_engine mssql integrations::mssql::tests::live \
  DBFREE_TEST_MSSQL_HOST=127.0.0.1 DBFREE_TEST_MSSQL_PORT=51433 DBFREE_TEST_MSSQL_USER=sa 'DBFREE_TEST_MSSQL_PASSWORD=DbFree!Passw0rd'

wanted mongodb && ready mongodb 5 && run_engine mongodb integrations::mongodb::tests::live \
  DB_FREE_MONGO_URL=mongodb://127.0.0.1:57017

wanted couchdb && ready couchdb 5 && run_engine couchdb integrations::couchdb::tests::live \
  DBFREE_TEST_COUCHDB_URL=http://127.0.0.1:55984 DBFREE_TEST_COUCHDB_USER=admin DBFREE_TEST_COUCHDB_PASSWORD=dbfree

wanted redis && ready redis 5 && run_engine redis integrations::redis::tests::live \
  DB_FREE_REDIS_URL=redis://127.0.0.1:56379/15

wanted valkey && ready valkey 5 && run_engine valkey integrations::redis::tests::live \
  DB_FREE_REDIS_URL=redis://127.0.0.1:56380/15

wanted memcached && ready memcached 5 && run_engine memcached integrations::memcached::tests::live \
  DBFREE_TEST_MEMCACHED_URL=127.0.0.1:51211

wanted dynamodb && ready dynamodb 5 && run_engine dynamodb integrations::dynamodb::tests::live \
  DBFREE_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:58000 DBFREE_TEST_DYNAMODB_REGION=us-east-1 \
  DBFREE_TEST_DYNAMODB_KEY=dummy DBFREE_TEST_DYNAMODB_SECRET=dummy

wanted cassandra && ready cassandra 5 && run_engine cassandra integrations::cassandra::tests::live \
  DBFREE_TEST_CASSANDRA_HOST=127.0.0.1 DBFREE_TEST_CASSANDRA_PORT=59042

wanted hbase && ready hbase 5 && run_engine hbase integrations::hbase::tests::live \
  DBFREE_TEST_HBASE_URL=http://127.0.0.1:58080

wanted neo4j && ready neo4j 5 && run_engine neo4j integrations::neo4j::tests::live \
  DBFREE_TEST_NEO4J_HOST=127.0.0.1 DBFREE_TEST_NEO4J_PORT=57687 DBFREE_TEST_NEO4J_USER=neo4j DBFREE_TEST_NEO4J_PASSWORD=dbfreepass

wanted influxdb && ready influxdb 5 && run_engine influxdb integrations::influxdb::tests::live \
  DBFREE_TEST_INFLUXDB_URL=http://127.0.0.1:58086 DBFREE_TEST_INFLUXDB_ORG=dbfree \
  DBFREE_TEST_INFLUXDB_BUCKET=metrics DBFREE_TEST_INFLUXDB_TOKEN=dbfreetoken

if wanted prometheus && ready prometheus 5; then
  # Prometheus only has series once it has scraped itself at least once.
  for _ in $(seq 60); do
    [ "$(curl -s 'http://127.0.0.1:59090/api/v1/query?query=up' | grep -c '"value"')" != "0" ] && break
    sleep 1
  done
  run_engine prometheus integrations::prometheus::tests::live DBFREE_TEST_PROMETHEUS_URL=http://127.0.0.1:59090
fi

if wanted victoriametrics && ready victoriametrics 5; then
  for _ in $(seq 60); do
    [ "$(curl -s 'http://127.0.0.1:58428/api/v1/query?query=up' | grep -c '"value"')" != "0" ] && break
    sleep 1
  done
  run_engine victoriametrics integrations::prometheus::tests::live \
    DBFREE_TEST_PROMETHEUS_URL=http://127.0.0.1:58428 DBFREE_TEST_PROMETHEUS_VM=1 DBFREE_TEST_PROMETHEUS_METRIC=vm_app_uptime_seconds
fi

wanted qdrant && ready qdrant 5 && run_engine qdrant integrations::qdrant::tests::live \
  DBFREE_TEST_QDRANT_URL=http://127.0.0.1:56333

wanted chroma && ready chroma 5 && run_engine chroma integrations::chroma::tests::live \
  DBFREE_TEST_CHROMA_URL=http://127.0.0.1:58001

wanted weaviate && ready weaviate 5 && run_engine weaviate integrations::weaviate::tests::live \
  DBFREE_TEST_WEAVIATE_URL=http://127.0.0.1:58081

wanted milvus && ready milvus 5 && run_engine milvus integrations::milvus::tests::live \
  DBFREE_TEST_MILVUS_URL=http://127.0.0.1:59091

wanted elasticsearch && ready elasticsearch 5 && run_engine elasticsearch integrations::elasticsearch::tests::live \
  DBFREE_TEST_ELASTICSEARCH_URL=http://127.0.0.1:59200

wanted opensearch && ready opensearch 5 && run_engine opensearch integrations::elasticsearch::tests::live \
  DBFREE_TEST_ELASTICSEARCH_URL=http://127.0.0.1:59201 DBFREE_TEST_ELASTICSEARCH_OPENSEARCH=1

wanted meilisearch && ready meilisearch 5 && run_engine meilisearch integrations::meilisearch::tests::live \
  DBFREE_TEST_MEILISEARCH_URL=http://127.0.0.1:57700 DBFREE_TEST_MEILISEARCH_KEY=dbfreekey

wanted typesense && ready typesense 5 && run_engine typesense integrations::typesense::tests::live \
  DBFREE_TEST_TYPESENSE_URL=http://127.0.0.1:58108 DBFREE_TEST_TYPESENSE_KEY=dbfreekey

wanted arangodb && ready arangodb 5 && run_engine arangodb integrations::arangodb::tests::live \
  DBFREE_TEST_ARANGODB_URL=http://127.0.0.1:58529 DBFREE_TEST_ARANGODB_USER=root DBFREE_TEST_ARANGODB_PASSWORD=dbfree

wanted surrealdb && ready surrealdb 5 && run_engine surrealdb integrations::surrealdb::tests::live \
  DBFREE_TEST_SURREALDB_URL=http://127.0.0.1:58002 DBFREE_TEST_SURREALDB_USER=root DBFREE_TEST_SURREALDB_PASSWORD=dbfree

if wanted orientdb && ready orientdb 5; then
  # OrientDB ships with no databases; the adapter needs one to attach to.
  curl -s -o /dev/null -u root:dbfree -X POST "http://127.0.0.1:52480/database/dbfreetest/plocal/graph" --max-time 30
  run_engine orientdb integrations::orientdb::tests::live \
    DBFREE_TEST_ORIENTDB_URL=http://127.0.0.1:52480 DBFREE_TEST_ORIENTDB_USER=root \
    DBFREE_TEST_ORIENTDB_PASSWORD=dbfree DBFREE_TEST_ORIENTDB_DB=dbfreetest
fi

wanted clickhouse && ready clickhouse 5 && run_engine clickhouse integrations::clickhouse::tests::live \
  DB_FREE_CLICKHOUSE_URL=http://127.0.0.1:58123 DB_FREE_CLICKHOUSE_USER=default DB_FREE_CLICKHOUSE_PASSWORD=dbfree

wanted immudb && ready immudb 5 && run_engine immudb integrations::immudb::tests::live \
  DBFREE_TEST_IMMUDB_URL=http://127.0.0.1:58082 DBFREE_TEST_IMMUDB_USER=immudb DBFREE_TEST_IMMUDB_PASSWORD=immudb

wanted redpanda && ready redpanda 5 && run_engine redpanda integrations::kafka::tests::live \
  DBFREE_TEST_KAFKA_HOST=127.0.0.1 DBFREE_TEST_KAFKA_PORT=59092 DBFREE_TEST_KAFKA_CREATE_TOPIC=1

wanted existdb && ready existdb 5 && run_engine existdb integrations::existdb::tests::live \
  DBFREE_TEST_EXISTDB_URL=http://127.0.0.1:58083 DBFREE_TEST_EXISTDB_USER=admin DBFREE_TEST_EXISTDB_PASSWORD=

if wanted fuseki && ready fuseki 5; then
  # Create the dataset if the image did not (older tags ignore FUSEKI_DATASET_1).
  curl -s -o /dev/null -u admin:dbfree -X POST -d "dbName=ds&dbType=mem" "http://127.0.0.1:53030/\$/datasets" --max-time 20
  run_engine sparql integrations::sparql::tests::live \
    DBFREE_TEST_SPARQL_URL=http://127.0.0.1:53030 DBFREE_TEST_SPARQL_DATASET=ds \
    DBFREE_TEST_SPARQL_USER=admin DBFREE_TEST_SPARQL_PASSWORD=dbfree
fi

wanted oracle && ready oracle 5 && run_engine oracle integrations::oracle::tests::live \
  DBFREE_TEST_ORACLE_URL=127.0.0.1:51521 DBFREE_TEST_ORACLE_SERVICE=FREEPDB1 \
  DBFREE_TEST_ORACLE_USER=system DBFREE_TEST_ORACLE_PASSWORD=dbfree

wanted druid && ready druid 5 && run_engine druid integrations::druid::tests::live \
  DBFREE_TEST_DRUID_URL=http://127.0.0.1:58888

# ---------------------------------------------------------------- report
echo
echo "=================================================================="
printf 'passed  (%2d): %s\n' "${#PASS[@]}" "${PASS[*]:-none}"
printf 'failed  (%2d): %s\n' "${#FAIL[@]}" "${FAIL[*]:-none}"
printf 'skipped (%2d): %s\n' "${#SKIP[@]}" "${SKIP[*]:-none}"
echo "=================================================================="
[ ${#FAIL[@]} -eq 0 ]
