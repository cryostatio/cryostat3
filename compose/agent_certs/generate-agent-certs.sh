#!/bin/sh

set -x

CERTS_DIR=$(realpath "$(dirname "$0")")

SSL_KEYSTORE=agent-keystore.p12

SSL_KEYSTORE_PASS_FILE=keystore.pass

cleanup() {
    cd "$CERTS_DIR"
    rm $SSL_KEYSTORE $SSL_KEYSTORE_PASS_FILE agent-server.cer
    cd -
}

case "$1" in
    clean)
        cleanup
        exit 0
        ;;
    generate)
        ;;
    *)
        echo "Usage: $0 [clean|generate]"
        exit 1
        ;;
esac

set -e

genpass() {
    < /dev/urandom tr -dc _A-Z-a-z-0-9 | head -c32
}

SSL_KEYSTORE_PASS=$(genpass)

cd "$CERTS_DIR"
trap "cd -" EXIT

echo "$SSL_KEYSTORE_PASS" > $SSL_KEYSTORE_PASS_FILE

keytool \
    -genkeypair -v \
    -alias quarkus-test-agent \
    -dname "CN=quarkus-test-agent, O=Cryostat, C=CA" \
    -storetype PKCS12 \
    -validity 365 \
    -keyalg RSA \
    -storepass "$SSL_KEYSTORE_PASS" \
    -keystore "$SSL_KEYSTORE"

keytool \
    -exportcert -v \
    -alias  quarkus-test-agent \
    -keystore "$SSL_KEYSTORE" \
    -storepass "$SSL_KEYSTORE_PASS" \
    -file agent_server.cer

cp agent_server.cer "$CERTS_DIR/../../truststore/quarkus-test-agent.cer"
