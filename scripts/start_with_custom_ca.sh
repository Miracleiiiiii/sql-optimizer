#!/bin/zsh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMP_CA_DIR="/private/tmp/sql-optimizer-ca"
ROOT_CA_PEM="$TMP_CA_DIR/isrg-root-x1.pem"
CUSTOM_CA_PEM="$TMP_CA_DIR/custom-ca.pem"

mkdir -p "$TMP_CA_DIR"

security find-certificate -p -c "ISRG Root X1" \
  /System/Library/Keychains/SystemRootCertificates.keychain > "$ROOT_CA_PEM"

CERTIFI_PEM="$(python3 - <<'PY'
import certifi
print(certifi.where())
PY
)"

cat "$CERTIFI_PEM" "$ROOT_CA_PEM" > "$CUSTOM_CA_PEM"

echo "Using CA bundle: $CUSTOM_CA_PEM"
cd "$ROOT_DIR"
SSL_CERT_FILE="$CUSTOM_CA_PEM" uvicorn app.main:app --host 127.0.0.1 --port 8000
