#!/bin/bash

set -e

DIR="$(dirname "$(realpath "$0")")"
source "${DIR}/genpass.bash"

function banner() {
    echo   "+------------------------------------------+"
    printf "| %-40s |\n" "$(date)"
    echo   "|                                          |"
    printf "| %-40s |\n" "$@"
    echo   "+------------------------------------------+"
}

USRFILE="/tmp/jmxremote.access"
PWFILE="/tmp/jmxremote.password"
function createJmxCredentials() {
    if [ -z "$CRYOSTAT_RJMX_USER" ]; then
        CRYOSTAT_RJMX_USER="cryostat"
    fi
    if [ -z "$CRYOSTAT_RJMX_PASS" ]; then
        CRYOSTAT_RJMX_PASS="$(genpass)"
    fi

    echo -n "$CRYOSTAT_RJMX_USER $CRYOSTAT_RJMX_PASS" > "$PWFILE"
    chmod 400 "$PWFILE"
    echo -n "$CRYOSTAT_RJMX_USER readwrite" > "$USRFILE"
    chmod 400 "$USRFILE"
}

function importTrustStores() {
    echo "Running as id:$(id -u) group:$(id -g)"
    if [ -z "$CONF_DIR" ]; then
        CONF_DIR="/opt/cryostat.d"
    fi
    if [ -z "$SSL_TRUSTSTORE_DIR" ]; then
        SSL_TRUSTSTORE_DIR="/truststore"
    fi

    if [ ! -d "$SSL_TRUSTSTORE_DIR" ]; then
        banner "$SSL_TRUSTSTORE_DIR does not exist; no certificates to import"
        return 0
    elif [ ! "$(ls -A "$SSL_TRUSTSTORE_DIR")" ]; then
        banner "$SSL_TRUSTSTORE_DIR is empty; no certificates to import"
        return 0
    fi

    SSL_TRUSTSTORE_PASS="$(cat "${SSL_TRUSTSTORE_PASS_FILE:-$CONF_DIR/truststore.pass}")"

    find "$SSL_TRUSTSTORE_DIR" -type f | while IFS= read -r cert; do
        echo "Importing certificate $cert ..."

        keytool -importcert -v \
            -noprompt \
            -alias "imported-$(basename "$cert")" \
            -trustcacerts \
            -keystore "${SSL_TRUSTSTORE:-$CONF_DIR/truststore.p12}" \
            -file "$cert"\
            -storepass "$SSL_TRUSTSTORE_PASS"
    done

    FLAGS+=(
        "-Djavax.net.ssl.trustStore=$SSL_TRUSTSTORE"
        "-Djavax.net.ssl.trustStorePassword=$SSL_TRUSTSTORE_PASS"
    )
}

FLAGS=()
importTrustStores

if [ "$CRYOSTAT_DISABLE_JMX_AUTH" = "true" ]; then
    banner "JMX Auth Disabled"
    FLAGS+=("-Dcom.sun.management.jmxremote.authenticate=false")
else
    createJmxCredentials
    FLAGS+=("-Dcom.sun.management.jmxremote.authenticate=true")
    FLAGS+=("-Dcom.sun.management.jmxremote.password.file=$PWFILE")
    FLAGS+=("-Dcom.sun.management.jmxremote.access.file=$USRFILE")
fi

export JAVA_OPTS_APPEND="${JAVA_OPTS_APPEND} ${FLAGS[*]}"
exec $1
