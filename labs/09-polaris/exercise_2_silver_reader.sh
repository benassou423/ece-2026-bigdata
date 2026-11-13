#!/usr/bin/env bash
# Exercise 2 of the Polaris lab: a restricted catalog role `silver_reader`.
#
# Usage:
#   exercise_2_silver_reader.sh setup     create the principal `silver_user`, the principal role `silver_analyst`,
#                                         the catalog role `silver_reader` with 5 grants on the namespace `silver`,
#                                         and the two assignments; the credential of `silver_user` is appended to
#                                         /home/onyxia/.polaris-credentials.env as POLARIS_SILVER_USER_CREDENTIAL
#                                         (never printed), then the three checks are listed
#   exercise_2_silver_reader.sh cleanup   delete the five objects in reverse order, remove the credential from the
#                                         file, then list the principals, the principal roles and the catalog roles
#
# Environment:
#   POLARIS_URI          URL of Polaris, default http://polaris:8181
#   POLARIS_CATALOG      name of the Polaris catalog, default $KUBERNETES_NAMESPACE
#   POLARIS_ROOT_SECRET  client secret of the `root` principal, default: the demo value of the deployment lab
#
# Dependencies: curl and jq. Each call prints one line `METHOD path -> HTTP status`, the path being relative to
# the management API, $POLARIS_URI/api/management/v1.

set -euo pipefail

POLARIS_URI="${POLARIS_URI:-http://polaris:8181}"
POLARIS_CATALOG="${POLARIS_CATALOG:-${KUBERNETES_NAMESPACE:-}}"
CREDENTIALS_FILE="/home/onyxia/.polaris-credentials.env"
API="$POLARIS_URI/api/management/v1"

usage() {
  echo "usage: $(basename "$0") setup|cleanup" >&2
  echo "environment: POLARIS_URI (default http://polaris:8181), POLARIS_CATALOG (default \$KUBERNETES_NAMESPACE)," >&2
  echo "             POLARIS_ROOT_SECRET (client secret of root)" >&2
  exit 2
}

[ "$#" -eq 1 ] || usage
case "$1" in
  setup | cleanup) ;;
  *) usage ;;
esac

if [ -z "$POLARIS_CATALOG" ]; then
  echo "POLARIS_CATALOG is not set and KUBERNETES_NAMESPACE is empty" >&2
  exit 1
fi
C="/catalogs/$POLARIS_CATALOG"

# Response of the last call. It may hold a credential: removed on exit.
RESPONSE="$(mktemp)"
trap 'rm -f "$RESPONSE"' EXIT

# Root token, as in the "Retrieve root level token" block of the deployment lab.
TOKEN="$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id=root \
  -d client_secret="${POLARIS_ROOT_SECRET:-s3cr3t}" \
  -d scope=PRINCIPAL_ROLE:ALL \
  | jq -r .access_token)"
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "could not get the root token from $POLARIS_URI" >&2
  exit 1
fi

# call METHOD PATH [JSON_BODY]: one call of the management API, the response goes to $RESPONSE.
call() {
  local method="$1" path="$2" body="${3:-}" status
  local args=(-s -o "$RESPONSE" -w '%{http_code}' -X "$method"
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")
  if [ -n "$body" ]; then
    args+=(-d "$body")
  fi
  status="$(curl "${args[@]}" "$API$path")"
  echo "$method $path -> HTTP $status"
  case "$status" in
    2??) ;;
    *)
      echo "unexpected status, response: $(head -c 300 "$RESPONSE")" >&2
      exit 1
      ;;
  esac
}

setup() {
  call POST /principals \
    '{"principal":{"name":"silver_user","properties":{}},"credentialRotationRequired":false}'
  # The credential is only returned at creation: appended to the file without being printed.
  jq -r '"POLARIS_SILVER_USER_CREDENTIAL=" + (.credentials.clientId + ":" + .credentials.clientSecret | @sh)' \
    "$RESPONSE" >>"$CREDENTIALS_FILE"

  call POST /principal-roles \
    '{"principalRole":{"name":"silver_analyst","properties":{}}}'
  call PUT /principals/silver_user/principal-roles \
    '{"principalRole":{"name":"silver_analyst"}}'
  call POST "$C/catalog-roles" \
    '{"catalogRole":{"name":"silver_reader","properties":{}}}'

  local privilege
  for privilege in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
    call PUT "$C/catalog-roles/silver_reader/grants" \
      "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$privilege\"}}"
  done

  call PUT "/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG" \
    '{"catalogRole":{"name":"silver_reader"}}'

  echo "-- checks"
  call GET "$C/catalog-roles/silver_reader/grants"
  jq -c '[.grants[] | {type, namespace, privilege}]' "$RESPONSE"
  call GET /principals/silver_user/principal-roles
  jq -c '[.roles[].name]' "$RESPONSE"
  call GET "/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG"
  jq -c '[.roles[].name]' "$RESPONSE"
}

cleanup() {
  call DELETE "/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG/silver_reader"
  call DELETE "$C/catalog-roles/silver_reader"
  call DELETE /principals/silver_user/principal-roles/silver_analyst
  call DELETE /principals/silver_user
  call DELETE /principal-roles/silver_analyst

  if [ -f "$CREDENTIALS_FILE" ]; then
    sed -i '/^POLARIS_SILVER_USER_CREDENTIAL=/d' "$CREDENTIALS_FILE"
  fi

  echo "-- lists"
  call GET /principals
  jq -c '[.principals[].name]' "$RESPONSE"
  call GET /principal-roles
  jq -c '[.roles[].name]' "$RESPONSE"
  call GET "$C/catalog-roles"
  jq -c '[.roles[].name]' "$RESPONSE"
}

"$1"
