#!/usr/bin/env bash
#
# Phoenix Adapters DynamoDB validation suite.
#
# Hits every supported API against the dockerized REST server and asserts
# the expected behaviour. Prints each request, response, and assertion in
# a readable format. Exits 0 on full pass; exits non-zero on the first
# failed assertion (and dumps the offending response).
#
# Usage:  docker/scripts/smoke.sh [label]
#
# Requires the cluster to already be up (see docker/README.md).
# Requires: jq, curl.
#
set -euo pipefail

URL="${PHX_URL:-http://localhost:8842}"
LABEL="${1:-}"
TBL="Smoke${LABEL}"
CT='Content-Type: application/x-amz-json-1.0'
TARGET='X-Amz-Target: DynamoDB_20120810'
TOTAL=18

if ! command -v jq >/dev/null 2>&1; then
    echo "smoke.sh: jq is required but not on PATH" >&2
    exit 2
fi

# ─── ANSI helpers ────────────────────────────────────────────────────────────
B='\033[1m'      # bold
DIM='\033[2m'    # dim
CYAN='\033[1;36m'
GREEN='\033[32m'
RED='\033[31m'
RESET='\033[0m'
RULE='─────────────────────────────────────────────────────────────'
BAR='━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'

STEP=0
PASS=0

banner() {
    printf "\n${CYAN}%s${RESET}\n" "$BAR"
    printf "${CYAN}  %s${RESET}\n" "$1"
    while [[ $# -gt 1 ]]; do shift; printf "${CYAN}  %s${RESET}\n" "$1"; done
    printf "${CYAN}%s${RESET}\n" "$BAR"
}

step() {
    STEP=$((STEP + 1))
    printf "\n${CYAN}[%2d/%2d]${RESET}  ${B}%s${RESET}\n" "$STEP" "$TOTAL" "$1"
    printf "${DIM}%s${RESET}\n" "$RULE"
}

show_json() {
    local label="$1" body="$2"
    printf "  ${DIM}%s:${RESET}\n" "$label"
    if printf '%s' "$body" | jq . >/dev/null 2>&1; then
        printf '%s' "$body" | jq . | sed 's/^/    /'
    else
        printf "    %s\n" "$body"
    fi
}

LAST_RESP=""

# Prints request + response visually and stashes the raw JSON in LAST_RESP.
ddb() {
    local action="$1" body="$2"
    show_json "request " "$body"
    LAST_RESP=$(curl -sS -X POST "$URL/" -H "$CT" -H "$TARGET.$action" -d "$body")
    show_json "response" "$LAST_RESP"
}

assert_eq() {
    local label="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        printf "  ${GREEN}✓${RESET}  %s ${B}==${RESET} %s\n" "$label" "$expected"
        PASS=$((PASS + 1))
    else
        printf "  ${RED}✗${RESET}  %s ${B}expected${RESET} %s, ${B}got${RESET} %s\n" \
            "$label" "$expected" "$actual" >&2
        exit 1
    fi
}

assert_nonempty() {
    local label="$1" value="$2"
    if [[ -n "$value" && "$value" != "null" ]]; then
        printf "  ${GREEN}✓${RESET}  %s present (%s)\n" "$label" "$value"
        PASS=$((PASS + 1))
    else
        printf "  ${RED}✗${RESET}  %s missing\n" "$label" >&2
        exit 1
    fi
}

assert_ge() {
    local label="$1" actual="$2" threshold="$3"
    if [[ "$actual" -ge "$threshold" ]]; then
        printf "  ${GREEN}✓${RESET}  %s ${B}>=${RESET} %s (got %s)\n" "$label" "$threshold" "$actual"
        PASS=$((PASS + 1))
    else
        printf "  ${RED}✗${RESET}  %s expected >= %s, got %s\n" "$label" "$threshold" "$actual" >&2
        exit 1
    fi
}

banner "Phoenix Adapters DynamoDB Validation Suite" \
       "Endpoint : $URL" \
       "Table    : $TBL"

# ─── Confirm the REST server is up before exercising the API ────────────────
# When the stack is launched with `docker compose up --wait` the
# phoenix-adapters-rest healthcheck has already ensured readiness; this
# check returns almost immediately in that case. Otherwise we probe
# ListTables until it responds (cold-start bootstrap takes ~30-60s).
TIMEOUT=180
SPIN=( '⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏' )
ready=false
printf "\n"
for i in $(seq 1 $TIMEOUT); do
    if curl -fs -m 3 -X POST "$URL/" \
            -H "$CT" -H "$TARGET.ListTables" -d '{}' >/dev/null 2>&1; then
        printf "\r${GREEN}✓${RESET} REST server is ready at %s (verified in %ds)                    \n" "$URL" "$i"
        ready=true
        break
    fi
    printf "\r${DIM}%s${RESET} Confirming REST server is ready at %s ${DIM}(%ds elapsed)${RESET}" \
        "${SPIN[$((i % ${#SPIN[@]}))]}" "$URL" "$i"
    sleep 1
done
if ! $ready; then
    printf "\n${RED}✗ REST server did not become ready within %ds at %s${RESET}\n" "$TIMEOUT" "$URL" >&2
    printf "${DIM}Last 30 lines of phx-adapters-rest:${RESET}\n" >&2
    docker logs phx-adapters-rest 2>&1 | tail -30 >&2 || true
    exit 1
fi

###############################################################################
# CRUD
###############################################################################

step "ListTables  (baseline)"
ddb ListTables '{}'

step "CreateTable  (streams enabled, NEW_AND_OLD_IMAGES)"
ddb CreateTable "$(cat <<EOF
{
  "TableName": "$TBL",
  "AttributeDefinitions": [{"AttributeName":"id","AttributeType":"S"}],
  "KeySchema":            [{"AttributeName":"id","KeyType":"HASH"}],
  "BillingMode": "PAY_PER_REQUEST",
  "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
}
EOF
)"
assert_eq "TableStatus" "$(jq -r '.TableDescription.TableStatus' <<<"$LAST_RESP")" "ACTIVE"

step "DescribeTable"
ddb DescribeTable "{\"TableName\":\"$TBL\"}"
assert_eq "StreamSpecification.StreamEnabled"  "$(jq -r '.Table.StreamSpecification.StreamEnabled'  <<<"$LAST_RESP")" "true"
assert_eq "StreamSpecification.StreamViewType" "$(jq -r '.Table.StreamSpecification.StreamViewType' <<<"$LAST_RESP")" "NEW_AND_OLD_IMAGES"
assert_nonempty "LatestStreamArn" "$(jq -r '.Table.LatestStreamArn // empty' <<<"$LAST_RESP")"

step "PutItem  id=a  (Alice, score=10)"
ddb PutItem "{\"TableName\":\"$TBL\",\"Item\":{\"id\":{\"S\":\"a\"},\"name\":{\"S\":\"Alice\"},\"score\":{\"N\":\"10\"}}}"

step "UpdateItem  id=a  (SET score=20, bonus=5, ReturnValues=ALL_NEW)"
ddb UpdateItem "$(cat <<EOF
{
  "TableName": "$TBL",
  "Key":              {"id": {"S": "a"}},
  "UpdateExpression": "SET score = :s, bonus = :b",
  "ExpressionAttributeValues": {":s": {"N":"20"}, ":b": {"N":"5"}},
  "ReturnValues": "ALL_NEW"
}
EOF
)"
assert_eq "Attributes.score.N" "$(jq -r '.Attributes.score.N' <<<"$LAST_RESP")" "20"
assert_eq "Attributes.bonus.N" "$(jq -r '.Attributes.bonus.N' <<<"$LAST_RESP")" "5"

step "GetItem  id=a"
ddb GetItem "{\"TableName\":\"$TBL\",\"Key\":{\"id\":{\"S\":\"a\"}}}"
assert_eq "Item.name.S"  "$(jq -r '.Item.name.S'  <<<"$LAST_RESP")" "Alice"
assert_eq "Item.score.N" "$(jq -r '.Item.score.N' <<<"$LAST_RESP")" "20"
assert_eq "Item.bonus.N" "$(jq -r '.Item.bonus.N' <<<"$LAST_RESP")" "5"

step "PutItem  id=b  (Bob, score=7)"
ddb PutItem "{\"TableName\":\"$TBL\",\"Item\":{\"id\":{\"S\":\"b\"},\"name\":{\"S\":\"Bob\"},\"score\":{\"N\":\"7\"}}}"

step "Scan"
ddb Scan "{\"TableName\":\"$TBL\"}"
assert_eq "Count" "$(jq -r '.Count' <<<"$LAST_RESP")" "2"

step "Query  id = 'a'"
ddb Query "$(cat <<EOF
{
  "TableName": "$TBL",
  "KeyConditionExpression": "id = :v",
  "ExpressionAttributeValues": {":v": {"S": "a"}}
}
EOF
)"
assert_eq "Count"           "$(jq -r '.Count'           <<<"$LAST_RESP")" "1"
assert_eq "Items[0].name.S" "$(jq -r '.Items[0].name.S' <<<"$LAST_RESP")" "Alice"

step "DeleteItem  id=b"
ddb DeleteItem "{\"TableName\":\"$TBL\",\"Key\":{\"id\":{\"S\":\"b\"}}}"

step "Scan  (after delete)"
ddb Scan "{\"TableName\":\"$TBL\"}"
assert_eq "Count" "$(jq -r '.Count' <<<"$LAST_RESP")" "1"

step "BatchWriteItem  (put id=c, id=d; delete id=a)"
ddb BatchWriteItem "$(cat <<EOF
{
  "RequestItems": {
    "$TBL": [
      {"PutRequest":    {"Item": {"id": {"S": "c"}, "name": {"S": "Carol"}}}},
      {"PutRequest":    {"Item": {"id": {"S": "d"}, "name": {"S": "Dan"}}}},
      {"DeleteRequest": {"Key":  {"id": {"S": "a"}}}}
    ]
  }
}
EOF
)"
assert_eq "UnprocessedItems  (size)" "$(jq -r '.UnprocessedItems // {} | length' <<<"$LAST_RESP")" "0"

step "Scan  (drain all pages after batch)"
total=0; iter_key=""
for page in $(seq 1 10); do
    if [[ -z "$iter_key" ]]; then
        ddb Scan "{\"TableName\":\"$TBL\"}"
    else
        ddb Scan "{\"TableName\":\"$TBL\",\"ExclusiveStartKey\":$iter_key}"
    fi
    n=$(jq -r '.Count // 0' <<<"$LAST_RESP")
    total=$((total + n))
    printf "  ${DIM}page %d: %d item(s)${RESET}\n" "$page" "$n"
    iter_key=$(jq -c '.LastEvaluatedKey // empty' <<<"$LAST_RESP")
    [[ -z "$iter_key" ]] && break
done
assert_eq "total Items across all pages" "$total" "2"

###############################################################################
# Streams API
###############################################################################

step "ListStreams"
ddb ListStreams "{\"TableName\":\"$TBL\"}"
listed_arn=$(jq -r ".Streams[]? | select(.TableName == \"$TBL\") | .StreamArn" <<<"$LAST_RESP" | head -n1)
assert_nonempty "StreamArn for $TBL" "$listed_arn"

step "DescribeStream  (poll until StreamStatus==ENABLED, max 30s)"
shard_id=""; status=""
for attempt in $(seq 1 15); do
    ddb DescribeStream "{\"StreamArn\":\"$listed_arn\"}"
    status=$(jq -r '.StreamDescription.StreamStatus // empty' <<<"$LAST_RESP")
    if [[ "$status" == "ENABLED" ]]; then
        shard_id=$(jq -r '.StreamDescription.Shards[0].ShardId // empty' <<<"$LAST_RESP")
        break
    fi
    printf "  ${DIM}attempt %d: status=%s${RESET}\n" "$attempt" "$status"
    sleep 2
done
assert_eq       "StreamDescription.StreamStatus" "$status"   "ENABLED"
assert_nonempty "StreamDescription.Shards[0].ShardId" "$shard_id"

step "GetShardIterator  (TRIM_HORIZON)"
ddb GetShardIterator "$(cat <<EOF
{
  "StreamArn":         "$listed_arn",
  "ShardId":           "$shard_id",
  "ShardIteratorType": "TRIM_HORIZON"
}
EOF
)"
iter=$(jq -r '.ShardIterator // empty' <<<"$LAST_RESP")
assert_nonempty "ShardIterator" "$iter"

step "GetRecords  (drain pages until empty)"
total=0; pages=0; seen_keys=""
while [[ -n "$iter" && "$iter" != "null" && $pages -lt 10 ]]; do
    pages=$((pages + 1))
    ddb GetRecords "{\"ShardIterator\":\"$iter\"}"
    n=$(jq -r '.Records | length' <<<"$LAST_RESP")
    keys=$(jq -r '.Records[]?.dynamodb.Keys.id.S' <<<"$LAST_RESP" | tr '\n' ',' | sed 's/,$//')
    [[ -n "$keys" ]] && seen_keys="${seen_keys:+$seen_keys,}$keys"
    total=$((total + n))
    printf "  ${DIM}page %d: %d record(s)  keys=[%s]${RESET}\n" "$pages" "$n" "$keys"
    next=$(jq -r '.NextShardIterator // empty' <<<"$LAST_RESP")
    if [[ "$next" == "$iter" || -z "$next" || "$next" == "null" ]]; then
        break
    fi
    iter="$next"
    [[ $n -eq 0 ]] && break
done
printf "  ${DIM}total records: %d  keys=[%s]${RESET}\n" "$total" "$seen_keys"
# Expect >= 4 mutations (PutItem-a, UpdateItem-a, PutItem-b, DeleteItem-b) plus
# 3 from the batch (delete-a, put-c, put-d).
assert_ge "stream record count" "$total" "4"

step "DeleteTable  (cleanup)"
ddb DeleteTable "{\"TableName\":\"$TBL\"}"

###############################################################################
# Summary
###############################################################################

banner "Result: ${PASS} checks PASSED across ${TOTAL} API calls"
