#!/usr/bin/env bash
# SOT: live-adapter-smoke-tests, docker-fixtures
#
# WHAT:  Starts a throwaway container per engine, waits for it to answer, then
#        runs that engine's `#[ignore]`-free but env-gated live test.
# WHY:   Unit tests prove the request/response shaping; only a real server
#        proves the adapter actually connects, lists and pages.
# HOW:   ./scripts/live-tests.sh [engine …]   (no args = every engine below)
#        Containers are named dbfree-<engine>, bind high ports (5xxxx) so they
#        never collide with a developer's own stack, and are removed on exit.
set -uo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"
NAMES=()
PASSED=()
FAILED=()
SKIPPED=()

cleanup() {
  for n in "${NAMES[@]:-}"; do
    [ -n "$n" ] && docker rm -f "dbfree-$n" >/dev/null 2>&1
  done
}
trap cleanup EXIT

start() { # start <name> <docker args…>
  local name="$1"; shift
  NAMES+=("$name")
  docker rm -f "dbfree-$name" >/dev/null 2>&1
  docker run --rm -d --name "dbfree-$name" "$@" >/dev/null 2>&1
}

wait_http() { # wait_http <url> <seconds> [expected-status-regex]
  local url="$1" secs="${2:-60}" ok="${3:-2..|401|404}"
  for _ in $(seq "$secs"); do
    local code
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$url" 2>/dev/null)
    [[ "$code" =~ ^($ok)$ ]] && return 0
    sleep 1
  done
  return 1
}

wait_tcp() { # wait_tcp <host> <port> <seconds>
  for _ in $(seq "${3:-60}"); do
    nc -z "$1" "$2" >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

wait_log() { # wait_log <name> <pattern> <seconds>
  for _ in $(seq "${3:-120}"); do
    docker logs "dbfree-$1" 2>&1 | grep -q "$2" && return 0
    sleep 1
  done
  return 1
}

run_test() { # run_test <engine> <cargo test filter>
  local engine="$1" filter="$2"
  echo "--- running: $filter"
  if (cd "$ROOT/src-tauri" && cargo test --lib "$filter" -- --nocapture --test-threads=1) 2>&1 | tail -25; then
    PASSED+=("$engine")
  else
    FAILED+=("$engine")
  fi
}

want() { # want <engine>  -> is this engine selected?
  [ "$#" -eq 0 ] && return 0
  [ "${SELECTED:-all}" = "all" ] && return 0
  [[ " $SELECTED " == *" $1 "* ]]
}

SELECTED="${*:-all}"

# ---------------------------------------------------------------- memcached
if want memcached; then
  echo "=== memcached"
  if start memcached -p 51211:11211 memcached:alpine && wait_tcp 127.0.0.1 51211 30; then
    DBFREE_TEST_MEMCACHED_URL=127.0.0.1:51211 run_test memcached integrations::memcached::tests::live_round_trip_when_configured
  else SKIPPED+=("memcached"); fi
  docker rm -f dbfree-memcached >/dev/null 2>&1
fi

# ---------------------------------------------------------------- qdrant
if want qdrant; then
  echo "=== qdrant"
  if start qdrant -p 56333:6333 qdrant/qdrant:latest && wait_http http://127.0.0.1:56333/healthz 60; then
    DBFREE_TEST_QDRANT_URL=http://127.0.0.1:56333 run_test qdrant integrations::qdrant::tests::live_round_trip_when_configured
  else SKIPPED+=("qdrant"); fi
  docker rm -f dbfree-qdrant >/dev/null 2>&1
fi

# ---------------------------------------------------------------- meilisearch
if want meilisearch; then
  echo "=== meilisearch"
  if start meilisearch -p 57700:7700 -e MEILI_MASTER_KEY=dbfreekey -e MEILI_ENV=development getmeili/meilisearch:latest \
     && wait_http http://127.0.0.1:57700/health 60; then
    DBFREE_TEST_MEILISEARCH_URL=http://127.0.0.1:57700 DBFREE_TEST_MEILISEARCH_KEY=dbfreekey \
      run_test meilisearch integrations::meilisearch::tests::live_round_trip_when_configured
  else SKIPPED+=("meilisearch"); fi
  docker rm -f dbfree-meilisearch >/dev/null 2>&1
fi

# ---------------------------------------------------------------- typesense
if want typesense; then
  echo "=== typesense"
  if start typesense -p 58108:8108 typesense/typesense:27.1 --data-dir /tmp --api-key=dbfreekey --enable-cors \
     && wait_http http://127.0.0.1:58108/health 60; then
    DBFREE_TEST_TYPESENSE_URL=http://127.0.0.1:58108 DBFREE_TEST_TYPESENSE_KEY=dbfreekey \
      run_test typesense integrations::typesense::tests::live_round_trip_when_configured
  else SKIPPED+=("typesense"); fi
  docker rm -f dbfree-typesense >/dev/null 2>&1
fi

# ---------------------------------------------------------------- chroma
if want chroma; then
  echo "=== chroma"
  if start chroma -p 58000:8000 chromadb/chroma:latest && wait_http http://127.0.0.1:58000/api/v2/heartbeat 90; then
    DBFREE_TEST_CHROMA_URL=http://127.0.0.1:58000 run_test chroma integrations::chroma::tests::live_round_trip_when_configured
  else SKIPPED+=("chroma"); fi
  docker rm -f dbfree-chroma >/dev/null 2>&1
fi

# ---------------------------------------------------------------- weaviate
if want weaviate; then
  echo "=== weaviate"
  if start weaviate -p 58080:8080 -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true -e PERSISTENCE_DATA_PATH=/tmp \
       -e DEFAULT_VECTORIZER_MODULE=none cr.weaviate.io/semitechnologies/weaviate:1.28.2 \
     && wait_http http://127.0.0.1:58080/v1/meta 90; then
    DBFREE_TEST_WEAVIATE_URL=http://127.0.0.1:58080 run_test weaviate integrations::weaviate::tests::live_round_trip_when_configured
  else SKIPPED+=("weaviate"); fi
  docker rm -f dbfree-weaviate >/dev/null 2>&1
fi

# ---------------------------------------------------------------- elasticsearch
if want elasticsearch; then
  echo "=== elasticsearch"
  if start elasticsearch -p 59200:9200 -e discovery.type=single-node -e xpack.security.enabled=false \
       -e ES_JAVA_OPTS="-Xms512m -Xmx512m" docker.elastic.co/elasticsearch/elasticsearch:8.15.0 \
     && wait_http http://127.0.0.1:59200 150; then
    DBFREE_TEST_ELASTICSEARCH_URL=http://127.0.0.1:59200 run_test elasticsearch integrations::elasticsearch::tests::live_round_trip_when_configured
  else SKIPPED+=("elasticsearch"); fi
  docker rm -f dbfree-elasticsearch >/dev/null 2>&1
fi

# ---------------------------------------------------------------- couchdb
if want couchdb; then
  echo "=== couchdb"
  if start couchdb -p 55984:5984 -e COUCHDB_USER=admin -e COUCHDB_PASSWORD=dbfree couchdb:3 \
     && wait_http http://127.0.0.1:55984 60; then
    DBFREE_TEST_COUCHDB_URL=http://127.0.0.1:55984 DBFREE_TEST_COUCHDB_USER=admin DBFREE_TEST_COUCHDB_PASSWORD=dbfree \
      run_test couchdb integrations::couchdb::tests::live_round_trip_when_configured
  else SKIPPED+=("couchdb"); fi
  docker rm -f dbfree-couchdb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- arangodb
if want arangodb; then
  echo "=== arangodb"
  if start arangodb -p 58529:8529 -e ARANGO_ROOT_PASSWORD=dbfree arangodb:3.12 \
     && wait_http http://127.0.0.1:58529/_api/version 90 "2..|401"; then
    DBFREE_TEST_ARANGODB_URL=http://127.0.0.1:58529 DBFREE_TEST_ARANGODB_USER=root DBFREE_TEST_ARANGODB_PASSWORD=dbfree \
      run_test arangodb integrations::arangodb::tests::live_round_trip_when_configured
  else SKIPPED+=("arangodb"); fi
  docker rm -f dbfree-arangodb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- surrealdb
if want surrealdb; then
  echo "=== surrealdb"
  if start surrealdb -p 58000:8000 surrealdb/surrealdb:latest start --user root --pass dbfree --bind 0.0.0.0:8000 \
     && wait_http http://127.0.0.1:58000/health 60; then
    DBFREE_TEST_SURREALDB_URL=http://127.0.0.1:58000 DBFREE_TEST_SURREALDB_USER=root DBFREE_TEST_SURREALDB_PASSWORD=dbfree \
      run_test surrealdb integrations::surrealdb::tests::live_round_trip_when_configured
  else SKIPPED+=("surrealdb"); fi
  docker rm -f dbfree-surrealdb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- neo4j
if want neo4j; then
  echo "=== neo4j"
  if start neo4j -p 57687:7687 -p 57474:7474 -e NEO4J_AUTH=neo4j/dbfreepass neo4j:5 \
     && wait_http http://127.0.0.1:57474 120 && wait_tcp 127.0.0.1 7687 60; then
    sleep 5
    DBFREE_TEST_NEO4J_HOST=127.0.0.1 DBFREE_TEST_NEO4J_PORT=57687 DBFREE_TEST_NEO4J_USER=neo4j DBFREE_TEST_NEO4J_PASSWORD=dbfreepass \
      run_test neo4j integrations::neo4j::tests::live_round_trip_when_configured
  else SKIPPED+=("neo4j"); fi
  docker rm -f dbfree-neo4j >/dev/null 2>&1
fi

# ---------------------------------------------------------------- cassandra
if want cassandra; then
  echo "=== cassandra"
  if start cassandra -p 59042:9042 -e CASSANDRA_NUM_TOKENS=1 -e HEAP_NEWSIZE=128M -e MAX_HEAP_SIZE=1024M cassandra:5 \
     && wait_log cassandra "Startup complete" 240; then
    sleep 5
    DBFREE_TEST_CASSANDRA_HOST=127.0.0.1 DBFREE_TEST_CASSANDRA_PORT=59042 run_test cassandra integrations::cassandra::tests::live_round_trip_when_configured
  else SKIPPED+=("cassandra"); fi
  docker rm -f dbfree-cassandra >/dev/null 2>&1
fi

# ---------------------------------------------------------------- kafka (redpanda)
if want kafka; then
  echo "=== kafka (redpanda)"
  if start kafka -p 59092:9092 docker.redpanda.com/redpandadata/redpanda:latest \
       redpanda start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M --node-id 0 --check=false \
       --kafka-addr PLAINTEXT://0.0.0.0:9092 --advertise-kafka-addr PLAINTEXT://127.0.0.1:59092 \
     && wait_tcp 127.0.0.1 9092 90; then
    sleep 5
    DBFREE_TEST_KAFKA_HOST=127.0.0.1 DBFREE_TEST_KAFKA_PORT=59092 DBFREE_TEST_KAFKA_CREATE_TOPIC=1 run_test kafka integrations::kafka::tests::live_round_trip_when_configured
  else SKIPPED+=("kafka"); fi
  docker rm -f dbfree-kafka >/dev/null 2>&1
fi

# ---------------------------------------------------------------- influxdb
if want influxdb; then
  echo "=== influxdb"
  if start influxdb -p 58086:8086 -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=admin \
       -e DOCKER_INFLUXDB_INIT_PASSWORD=dbfreepass -e DOCKER_INFLUXDB_INIT_ORG=dbfree \
       -e DOCKER_INFLUXDB_INIT_BUCKET=metrics -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=dbfreetoken influxdb:2 \
     && wait_http http://127.0.0.1:58086/health 90; then
    DBFREE_TEST_INFLUXDB_URL=http://127.0.0.1:58086 DBFREE_TEST_INFLUXDB_ORG=dbfree \
    DBFREE_TEST_INFLUXDB_BUCKET=metrics DBFREE_TEST_INFLUXDB_TOKEN=dbfreetoken \
      run_test influxdb integrations::influxdb::tests::live_round_trip_when_configured
  else SKIPPED+=("influxdb"); fi
  docker rm -f dbfree-influxdb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- prometheus
if want prometheus; then
  echo "=== prometheus"
  if start prometheus -p 59090:9090 prom/prometheus:latest && wait_http http://127.0.0.1:59090/-/healthy 60; then
    # Prometheus only has series after it has scraped itself at least once.
    for _ in $(seq 60); do
      [ "$(curl -s "http://127.0.0.1:59090/api/v1/query?query=up" | grep -c '"value"')" != "0" ] && break
      sleep 1
    done
    DBFREE_TEST_PROMETHEUS_URL=http://127.0.0.1:59090 run_test prometheus integrations::prometheus::tests::live_round_trip_when_configured
  else SKIPPED+=("prometheus"); fi
  docker rm -f dbfree-prometheus >/dev/null 2>&1
fi

# ---------------------------------------------------------------- immudb
if want immudb; then
  echo "=== immudb"
  if start immudb -p 53322:3322 -p 53323:3323 -p 58080:8080 codenotary/immudb:latest \
     && wait_http http://127.0.0.1:53323 60 "2..|400|401|404|405"; then
    sleep 3
    DBFREE_TEST_IMMUDB_URL=http://127.0.0.1:53323 DBFREE_TEST_IMMUDB_USER=immudb DBFREE_TEST_IMMUDB_PASSWORD=immudb \
      run_test immudb integrations::immudb::tests::live_round_trip_when_configured
  else SKIPPED+=("immudb"); fi
  docker rm -f dbfree-immudb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- dynamodb-local
if want dynamodb; then
  echo "=== dynamodb (local)"
  if start dynamodb -p 58000:8000 amazon/dynamodb-local:latest && wait_tcp 127.0.0.1 8000 60; then
    DBFREE_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:58000 DBFREE_TEST_DYNAMODB_REGION=us-east-1 \
    DBFREE_TEST_DYNAMODB_KEY=dummy DBFREE_TEST_DYNAMODB_SECRET=dummy \
      run_test dynamodb integrations::dynamodb::tests::live_round_trip_when_configured
  else SKIPPED+=("dynamodb"); fi
  docker rm -f dbfree-dynamodb >/dev/null 2>&1
fi

# ---------------------------------------------------------------- fuseki (sparql)
if want sparql; then
  echo "=== sparql (fuseki)"
  if start sparql -p 53030:3030 -e ADMIN_PASSWORD=dbfree stain/jena-fuseki:latest \
     && wait_http http://127.0.0.1:53030/\$/ping 90 "2..|401"; then
    docker exec dbfree-sparql /jena-fuseki/bin/s-post http://localhost:3030/ds/data default /dev/null >/dev/null 2>&1
    DBFREE_TEST_SPARQL_URL=http://127.0.0.1:53030 DBFREE_TEST_SPARQL_DATASET=ds \
    DBFREE_TEST_SPARQL_USER=admin DBFREE_TEST_SPARQL_PASSWORD=dbfree \
      run_test sparql integrations::sparql::tests::live_round_trip_when_configured
  else SKIPPED+=("sparql"); fi
  docker rm -f dbfree-sparql >/dev/null 2>&1
fi

echo
echo "=================================================================="
echo "passed : ${PASSED[*]:-none}"
echo "failed : ${FAILED[*]:-none}"
echo "skipped: ${SKIPPED[*]:-none}"
[ ${#FAILED[@]} -eq 0 ]
