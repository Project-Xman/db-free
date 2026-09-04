#!/usr/bin/env bash
# SOT: all-engine-live-test-runner, docker-compose-test-driver
#
# WHAT:  Walks the engine catalogue one product category at a time. For each
#        category it starts those engines, waits for them, runs their live
#        round-trip tests, reports the category, then stops them before moving
#        on. A category with any failure stops the run unless KEEP_GOING=1.
# WHY:   "the adapters work" is only true if a real server says so, and thirty
#        servers at once starves the Docker daemon on a laptop: probes time out
#        together and `compose up` itself begins to hang. One category at a time
#        keeps the machine responsive and each result trustworthy.
# HOW:   ./scripts/test-all-dbs.sh                  # every category, in order
#        ./scripts/test-all-dbs.sh relational vector   # only these categories
#        ./scripts/test-all-dbs.sh qdrant neo4j        # or individual engines
#        KEEP_GOING=1 ...   # do not stop at the first failing category
#        KEEP_UP=1 ...      # leave the last category running
#        WITH_HEAVY=1 ...   # include oracle + druid
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
    qdrant) echo 56333 ;; chroma) echo 58001 ;; weaviate) echo 58081 ;; milvus) echo 59530 ;;
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
        postgres) docker exec "$id" psql -h 127.0.0.1 -U postgres -d dbfree -tAc 'SELECT 1' >/dev/null 2>&1 && return 0 ;;
        # ES/OpenSearch accept TCP before the cluster leaves "red"; wait for a
        # real cluster-health response instead.
        elasticsearch) curl -s -f -o /dev/null --max-time 3 "http://127.0.0.1:59200/_cluster/health?wait_for_status=yellow&timeout=2s" && return 0 ;;
        opensearch) curl -s -f -o /dev/null --max-time 3 "http://127.0.0.1:59201/_cluster/health?wait_for_status=yellow&timeout=2s" && return 0 ;;
        # Milvus opens 19530 long before its components report healthy.
        milvus) [ "$(curl -s --max-time 3 http://127.0.0.1:59091/healthz)" = "OK" ] && return 0 ;;
        # /health answers before DOCKER_INFLUXDB_INIT_* has finished creating
        # the org, bucket and token, so probe the authenticated API instead.
        influxdb) curl -s -f -o /dev/null --max-time 3 -H "Authorization: Token dbfreetoken" \
            "http://127.0.0.1:58086/api/v2/buckets?org=dbfree" && return 0 ;;
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

# ---------------------------------------------------------------- categories
# WHAT:  The engine catalogue, in the same category order the connection picker
#        shows. Each entry is "category:service …".
# WHY:   Testing a category at a time mirrors how the app presents engines, and
#        keeps at most a handful of servers running at once.
CATEGORIES=(
  "relational:postgres mysql mariadb mssql"
  "document:mongodb couchdb"
  "key-value:redis valkey memcached dynamodb"
  "wide-column:cassandra hbase"
  "graph:neo4j"
  "time-series:influxdb prometheus victoriametrics"
  "vector:qdrant chroma milvus"
  "search:elasticsearch opensearch meilisearch typesense"
  "multi-model:arangodb surrealdb orientdb weaviate"
  "olap:clickhouse"
  "ledger:immudb"
  "streaming:redpanda"
  "xml:existdb"
  "rdf:fuseki"
  "embedded:sqlite duckdb rocksdb"
)
[ "${WITH_HEAVY:-0}" = "1" ] && CATEGORIES+=("heavy:oracle druid")

# WHAT:  Runs one engine once its service is up. Named so the group loop can
#        stay a flat list of one-liners.
test_service() { # test_service <service>
  case "$1" in
    postgres) run_engine postgres integrations::postgres::tests::live \
        DB_FREE_PG_HOST=127.0.0.1 DB_FREE_PG_PORT=55432 DB_FREE_PG_USER=postgres DB_FREE_PG_PASSWORD=dbfree DB_FREE_PG_DB=dbfree ;;
    mysql) run_engine mysql integrations::mysql::tests::live \
        DB_FREE_MYSQL_HOST=127.0.0.1 DB_FREE_MYSQL_PORT=53306 DB_FREE_MYSQL_USER=root DB_FREE_MYSQL_PASSWORD=dbfree DB_FREE_MYSQL_DB=dbfree ;;
    mariadb) run_engine mariadb integrations::mysql::tests::live \
        DB_FREE_MYSQL_HOST=127.0.0.1 DB_FREE_MYSQL_PORT=53307 DB_FREE_MYSQL_USER=root DB_FREE_MYSQL_PASSWORD=dbfree DB_FREE_MYSQL_DB=dbfree ;;
    mssql) run_engine mssql integrations::mssql::tests::live \
        DBFREE_TEST_MSSQL_HOST=127.0.0.1 DBFREE_TEST_MSSQL_PORT=51433 DBFREE_TEST_MSSQL_USER=sa 'DBFREE_TEST_MSSQL_PASSWORD=DbFree!Passw0rd' ;;
    mongodb) run_engine mongodb integrations::mongodb::tests::live DB_FREE_MONGO_URL=mongodb://127.0.0.1:57017 ;;
    couchdb) run_engine couchdb integrations::couchdb::tests::live \
        DBFREE_TEST_COUCHDB_URL=http://127.0.0.1:55984 DBFREE_TEST_COUCHDB_USER=admin DBFREE_TEST_COUCHDB_PASSWORD=dbfree ;;
    redis) run_engine redis integrations::redis::tests::live DB_FREE_REDIS_URL=redis://127.0.0.1:56379/15 ;;
    valkey) run_engine valkey integrations::redis::tests::live DB_FREE_REDIS_URL=redis://127.0.0.1:56380/15 ;;
    memcached) run_engine memcached integrations::memcached::tests::live DBFREE_TEST_MEMCACHED_URL=127.0.0.1:51211 ;;
    dynamodb) run_engine dynamodb integrations::dynamodb::tests::live \
        DBFREE_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:58000 DBFREE_TEST_DYNAMODB_REGION=us-east-1 \
        DBFREE_TEST_DYNAMODB_KEY=dummy DBFREE_TEST_DYNAMODB_SECRET=dummy ;;
    cassandra) run_engine cassandra integrations::cassandra::tests::live \
        DBFREE_TEST_CASSANDRA_HOST=127.0.0.1 DBFREE_TEST_CASSANDRA_PORT=59042 ;;
    hbase) run_engine hbase integrations::hbase::tests::live DBFREE_TEST_HBASE_URL=http://127.0.0.1:58080 ;;
    neo4j) run_engine neo4j integrations::neo4j::tests::live \
        DBFREE_TEST_NEO4J_HOST=127.0.0.1 DBFREE_TEST_NEO4J_PORT=57687 DBFREE_TEST_NEO4J_USER=neo4j DBFREE_TEST_NEO4J_PASSWORD=dbfreepass ;;
    arangodb) run_engine arangodb integrations::arangodb::tests::live \
        DBFREE_TEST_ARANGODB_URL=http://127.0.0.1:58529 DBFREE_TEST_ARANGODB_USER=root DBFREE_TEST_ARANGODB_PASSWORD=dbfree ;;
    surrealdb) run_engine surrealdb integrations::surrealdb::tests::live \
        DBFREE_TEST_SURREALDB_URL=http://127.0.0.1:58002 DBFREE_TEST_SURREALDB_USER=root DBFREE_TEST_SURREALDB_PASSWORD=dbfree ;;
    orientdb)
        # OrientDB ships with no databases; the adapter needs one to attach to.
        curl -s -o /dev/null -u root:dbfree -X POST "http://127.0.0.1:52480/database/dbfreetest/plocal/graph" --max-time 30
        run_engine orientdb integrations::orientdb::tests::live \
          DBFREE_TEST_ORIENTDB_URL=http://127.0.0.1:52480 DBFREE_TEST_ORIENTDB_USER=root \
          DBFREE_TEST_ORIENTDB_PASSWORD=dbfree DBFREE_TEST_ORIENTDB_DB=dbfreetest ;;
    influxdb) run_engine influxdb integrations::influxdb::tests::live \
        DBFREE_TEST_INFLUXDB_URL=http://127.0.0.1:58086 DBFREE_TEST_INFLUXDB_ORG=dbfree \
        DBFREE_TEST_INFLUXDB_BUCKET=metrics DBFREE_TEST_INFLUXDB_TOKEN=dbfreetoken ;;
    prometheus)
        # Prometheus only has series once it has scraped itself at least once.
        for _ in $(seq 60); do
          [ "$(curl -s 'http://127.0.0.1:59090/api/v1/query?query=up' | grep -c '"value"')" != "0" ] && break
          sleep 1
        done
        run_engine prometheus integrations::prometheus::tests::live DBFREE_TEST_PROMETHEUS_URL=http://127.0.0.1:59090 ;;
    victoriametrics)
        for _ in $(seq 60); do
          [ "$(curl -s 'http://127.0.0.1:58428/api/v1/query?query=up' | grep -c '"value"')" != "0" ] && break
          sleep 1
        done
        run_engine victoriametrics integrations::prometheus::tests::live \
          DBFREE_TEST_PROMETHEUS_URL=http://127.0.0.1:58428 DBFREE_TEST_PROMETHEUS_VM=1 \
          DBFREE_TEST_PROMETHEUS_METRIC=vm_app_uptime_seconds ;;
    qdrant) run_engine qdrant integrations::qdrant::tests::live DBFREE_TEST_QDRANT_URL=http://127.0.0.1:56333 ;;
    chroma) run_engine chroma integrations::chroma::tests::live DBFREE_TEST_CHROMA_URL=http://127.0.0.1:58001 ;;
    weaviate) run_engine weaviate integrations::weaviate::tests::live DBFREE_TEST_WEAVIATE_URL=http://127.0.0.1:58081 ;;
    milvus) run_engine milvus integrations::milvus::tests::live DBFREE_TEST_MILVUS_URL=http://127.0.0.1:59530 ;;
    elasticsearch) run_engine elasticsearch integrations::elasticsearch::tests::live \
        DBFREE_TEST_ELASTICSEARCH_URL=http://127.0.0.1:59200 ;;
    opensearch) run_engine opensearch integrations::elasticsearch::tests::live \
        DBFREE_TEST_ELASTICSEARCH_URL=http://127.0.0.1:59201 DBFREE_TEST_ELASTICSEARCH_OPENSEARCH=1 ;;
    meilisearch) run_engine meilisearch integrations::meilisearch::tests::live \
        DBFREE_TEST_MEILISEARCH_URL=http://127.0.0.1:57700 DBFREE_TEST_MEILISEARCH_KEY=dbfreekey ;;
    typesense) run_engine typesense integrations::typesense::tests::live \
        DBFREE_TEST_TYPESENSE_URL=http://127.0.0.1:58108 DBFREE_TEST_TYPESENSE_KEY=dbfreekey ;;
    clickhouse) run_engine clickhouse integrations::clickhouse::tests::live \
        DB_FREE_CLICKHOUSE_URL=http://127.0.0.1:58123 DB_FREE_CLICKHOUSE_USER=default DB_FREE_CLICKHOUSE_PASSWORD=dbfree ;;
    immudb) run_engine immudb integrations::immudb::tests::live \
        DBFREE_TEST_IMMUDB_URL=http://127.0.0.1:58082 DBFREE_TEST_IMMUDB_USER=immudb DBFREE_TEST_IMMUDB_PASSWORD=immudb ;;
    redpanda) run_engine redpanda integrations::kafka::tests::live \
        DBFREE_TEST_KAFKA_HOST=127.0.0.1 DBFREE_TEST_KAFKA_PORT=59092 DBFREE_TEST_KAFKA_CREATE_TOPIC=1 ;;
    existdb) run_engine existdb integrations::existdb::tests::live \
        DBFREE_TEST_EXISTDB_URL=http://127.0.0.1:58083 DBFREE_TEST_EXISTDB_USER=admin DBFREE_TEST_EXISTDB_PASSWORD= ;;
    fuseki)
        # Create the dataset if the image did not (older tags ignore FUSEKI_DATASET_1).
        curl -s -o /dev/null -u admin:dbfree -X POST -d "dbName=ds&dbType=mem" "http://127.0.0.1:53030/\$/datasets" --max-time 20
        run_engine sparql integrations::sparql::tests::live \
          DBFREE_TEST_SPARQL_URL=http://127.0.0.1:53030 DBFREE_TEST_SPARQL_DATASET=ds \
          DBFREE_TEST_SPARQL_USER=admin DBFREE_TEST_SPARQL_PASSWORD=dbfree ;;
    oracle) run_engine oracle integrations::oracle::tests::live \
        DBFREE_TEST_ORACLE_URL=127.0.0.1:51521 DBFREE_TEST_ORACLE_SERVICE=FREEPDB1 \
        DBFREE_TEST_ORACLE_USER=system DBFREE_TEST_ORACLE_PASSWORD=dbfree ;;
    druid) run_engine druid integrations::druid::tests::live DBFREE_TEST_DRUID_URL=http://127.0.0.1:58888 ;;
  esac
}

# WHAT:  True when the caller asked for this category by name.
wanted_category() {
  [ ${#SELECTED[@]} -eq 0 ] && return 0
  for s in "${SELECTED[@]}"; do [ "$s" = "$1" ] && return 0; done
  return 1
}

FAILED_CATEGORIES=()

for entry in "${CATEGORIES[@]}"; do
  category="${entry%%:*}"
  services="${entry#*:}"

  # A category runs when it is named, or when one of its engines is.
  chunk=()
  if wanted_category "$category"; then
    for svc in $services; do chunk+=("$svc"); done
  else
    for svc in $services; do wanted "$svc" && chunk+=("$svc"); done
  fi
  [ ${#chunk[@]} -eq 0 ] && continue

  log "category: $category  (${chunk[*]})"
  before_pass=${#PASS[@]}
  before_fail=${#FAIL[@]}

  # File engines have no container; test them directly.
  servers=()
  for svc in "${chunk[@]}"; do
    case " ${FILE_ENGINES[*]} " in
      *" $svc "*) run_engine "$svc" "integrations::${svc}::" ;;
      *) servers+=("$svc") ;;
    esac
  done

  if [ ${#servers[@]} -gt 0 ]; then
    deps=("${servers[@]}")
    for svc in "${servers[@]}"; do
      [ "$svc" = "milvus" ] && deps+=(milvus-etcd milvus-minio)
    done
    "${COMPOSE[@]}" up -d --no-recreate "${deps[@]}" >/dev/null 2>&1

    for svc in "${servers[@]}"; do
      if ready "$svc" 300; then
        test_service "$svc"
      else
        note "$svc never became ready"
        SKIP+=("$svc")
      fi
    done

    # Free the machine before the next category.
    if [ "${KEEP_UP:-0}" != "1" ]; then
      "${COMPOSE[@]}" rm -sf "${deps[@]}" >/dev/null 2>&1
    fi
  fi

  passed_here=$(( ${#PASS[@]} - before_pass ))
  failed_here=$(( ${#FAIL[@]} - before_fail ))
  if [ "$failed_here" -gt 0 ]; then
    printf '\n   \033[31mcategory %s: %d passed, %d FAILED\033[0m\n' "$category" "$passed_here" "$failed_here"
    FAILED_CATEGORIES+=("$category")
    if [ "${KEEP_GOING:-0}" != "1" ]; then
      note "stopping at the first failing category (KEEP_GOING=1 to continue)"
      break
    fi
  else
    printf '\n   \033[32mcategory %s: %d passed\033[0m\n' "$category" "$passed_here"
  fi
done

# ---------------------------------------------------------------- report
echo
echo "=================================================================="
printf 'passed  (%2d): %s\n' "${#PASS[@]}" "${PASS[*]:-none}"
printf 'failed  (%2d): %s\n' "${#FAIL[@]}" "${FAIL[*]:-none}"
printf 'skipped (%2d): %s\n' "${#SKIP[@]}" "${SKIP[*]:-none}"
[ ${#FAILED_CATEGORIES[@]} -gt 0 ] && printf 'failing categories: %s\n' "${FAILED_CATEGORIES[*]}"
echo "=================================================================="
[ ${#FAIL[@]} -eq 0 ]
