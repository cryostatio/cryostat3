#!/usr/bin/env bash

set -e

DIR="$(dirname "$(realpath "$0")")"
source "${DIR}/genpass.bash"

SSL_TRUSTSTORE_PASS="$(genpass)"

echo "$SSL_TRUSTSTORE_PASS" > "$SSL_TRUSTSTORE_PASS_FILE"

trap "cd -" EXIT
cd "$CONF_DIR"

keytool -importkeystore \
    -noprompt \
    -storetype PKCS12 \
    -srckeystore /usr/lib/jvm/jre-17-openjdk/lib/security/cacerts \
    -srcstorepass changeit \
    -destkeystore "$SSL_TRUSTSTORE" \
    -deststorepass "$SSL_TRUSTSTORE_PASS"

chmod 664 "${SSL_TRUSTSTORE}"
chmod 640 "${SSL_TRUSTSTORE_PASS_FILE}"
