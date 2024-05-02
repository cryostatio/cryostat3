#!/usr/bin/env bash

if ! command -v yq >/dev/null 2>&1 ; then
    echo "No 'yq' found"
    exit 1
fi

DIR="$(dirname "$(readlink -f "$0")")"
export DIR

FILES=(
    "${DIR}/compose/db.yml"
)

USE_USERHOSTS=${USE_USERHOSTS:-true}
PULL_IMAGES=${PULL_IMAGES:-true}
KEEP_VOLUMES=${KEEP_VOLUMES:-false}
OPEN_TABS=${OPEN_TABS:-false}

PRECREATE_BUCKETS=${PRECREATE_BUCKETS:-archivedrecordings,archivedreports,eventtemplates,probes}

CRYOSTAT_HTTP_HOST=${CRYOSTAT_HTTP_HOST:-cryostat}
CRYOSTAT_HTTP_PORT=${CRYOSTAT_HTTP_PORT:-8080}
USE_PROXY=${USE_PROXY:-true}
DEPLOY_GRAFANA=${DEPLOY_GRAFANA:-true}
DRY_RUN=${DRY_RUN:-false}

display_usage() {
    echo "Usage:"
    echo -e "\t-h\t\t\t\t\t\tprint this Help text."
    echo -e "\t-O\t\t\t\t\t\tOffline mode, do not attempt to pull container images."
    echo -e "\t-p\t\t\t\t\t\tDisable auth Proxy."
    echo -e "\t-s [seaweed|minio|cloudserver|localstack]\tS3 implementation to spin up (default \"seaweed\")."
    echo -e "\t-G\t\t\t\t\t\texclude Grafana dashboard and jfr-datasource from deployment."
    echo -e "\t-r\t\t\t\t\t\tconfigure a cryostat-Reports sidecar instance"
    echo -e "\t-t\t\t\t\t\t\tinclude sample applications for Testing."
    echo -e "\t-V\t\t\t\t\t\tdo not discard data storage Volumes on exit."
    echo -e "\t-X\t\t\t\t\t\tdeploy additional development aid tools."
    echo -e "\t-c [podman|docker]\t\t\t\tUse Podman or Docker Container Engine (default \"podman\")."
    echo -e "\t-b\t\t\t\t\t\tOpen a Browser tab for each running service's first mapped port (ex. auth proxy login, database viewer)"
    echo -e "\t-n\t\t\t\t\t\tDo Not apply configuration changes, instead emit the compose YAML that would have been used to stdout."
}

s3=seaweed
ce=podman
while getopts "hs:prGtOVXcbn" opt; do
    case $opt in
        h)
            display_usage
            exit 0
            ;;
        p)
            USE_PROXY=false
            ;;
        s)
            s3="${OPTARG}"
            ;;
        G)
            DEPLOY_GRAFANA=false
            ;;
        t)
            FILES+=("${DIR}/compose/sample-apps.yml")
            ;;
        O)
            PULL_IMAGES=false
            ;;
        V)
            KEEP_VOLUMES=true
            DATABASE_GENERATION=update
            ;;
        X)
            FILES+=("${DIR}/compose/db-viewer.yml")
            ;;
        c)
            ce="${OPTARG}"
            ;;
        b)
            OPEN_TABS=true
            ;;
        r)
            FILES+=('./compose/reports.yml')
            ;;
        n)
            DRY_RUN=true
            ;;
        *)
            display_usage
            exit 1
            ;;
    esac
done

if [ "${DEPLOY_GRAFANA}" = "true" ]; then
    FILES+=(
        "${DIR}/compose/cryostat-grafana.yml"
        "${DIR}/compose/jfr-datasource.yml"
    )
fi


if [ "${USE_PROXY}" = "true" ]; then
    FILES+=("${DIR}/compose/auth_proxy.yml")
    CRYOSTAT_HTTP_HOST=auth
    CRYOSTAT_HTTP_PORT=8181
else
    FILES+=("${DIR}/compose/no_proxy.yml")
    if [ "${s3}" != "none" ]; then
        FILES+=("${DIR}/compose/s3_no_proxy.yml")
    fi
    if [ "${DEPLOY_GRAFANA}" = "true" ]; then
      FILES+=("${DIR}/compose/grafana_no_proxy.yml")
    fi
    GRAFANA_DASHBOARD_EXT_URL=http://grafana:3000/
fi
export CRYOSTAT_HTTP_HOST
export CRYOSTAT_HTTP_PORT
export GRAFANA_DASHBOARD_EXT_URL
export DATABASE_GENERATION

s3Manifest="${DIR}/compose/s3-${s3}.yml"
if [ ! -f "${s3Manifest}" ]; then
    echo "Unknown S3 selection: ${s3}"
    display_usage
    exit 2
fi
FILES+=("${s3Manifest}")
if [ "${s3}" = "none" ]; then
    STORAGE_PORT="4566"
else
    STORAGE_PORT="$(yq '.services.*.expose[0]' "${s3Manifest}" | grep -v null)"
fi
export STORAGE_PORT
export PRECREATE_BUCKETS

unshift() {
    local -n ary=$1;
    shift;
    ary=("$@" "${ary[@]}");
}

if [ "${ce}" = "podman" ]; then
    unshift FILES "${DIR}/compose/cryostat.yml"
    container_engine="podman"
elif [ "${ce}" = "docker" ]; then
    unshift FILES "${DIR}/compose/cryostat_docker.yml"
    container_engine="docker"
else
    echo "Unknown Container Engine selection: ${ce}"
    display_usage
    exit 2
fi

if [ ! "${DRY_RUN}" = "true" ]; then
    set -xe
fi

CMD=()
for file in "${FILES[@]}"; do
    CMD+=(-f "${file}")
done

if [ "${DRY_RUN}" = "true" ]; then
    set +xe
    docker-compose \
        "${CMD[@]}" \
        config
    exit 0
fi

PIDS=()

HOSTSFILE="${HOSTSFILE:-$HOME/.hosts}"

cleanup() {
    set +xe
    local downFlags=('--remove-orphans')
    if [ "${KEEP_VOLUMES}" != "true" ]; then
        downFlags=('--volumes')
    fi
    docker-compose \
        "${CMD[@]}" \
        down "${downFlags[@]}"
    if [ "${USE_PROXY}" = "true" ]; then
        ${container_engine} rm proxy_cfg_helper || true
        ${container_engine} rm proxy_certs_helper || true
        ${container_engine} volume rm auth_proxy_cfg || true
        ${container_engine} volume rm auth_proxy_certs || true
    fi
    if [ "${s3}" = "localstack" ]; then
        ${container_engine} rm localstack_cfg_helper || true
        ${container_engine} volume rm localstack_cfg || true
    fi
    ${container_engine} rm jmxtls_cfg_helper || true
    ${container_engine} volume rm jmxtls_cfg || true
    ${container_engine} rm templates_helper || true
    ${container_engine} volume rm templates || true
    truncate -s 0 "${HOSTSFILE}"
    for i in "${PIDS[@]}"; do
        kill -0 "${i}" && kill "${i}"
    done
    set -xe
}
trap cleanup EXIT
cleanup

createProxyCfgVolume() {
    "${container_engine}" volume create auth_proxy_cfg
    "${container_engine}" container create --name proxy_cfg_helper -v auth_proxy_cfg:/tmp busybox
    local cfg
    cfg="$(mktemp)"
    chmod 644 "${cfg}"
    envsubst '$STORAGE_PORT' < "${DIR}/compose/auth_proxy_alpha_config.yaml" > "${cfg}"
    "${container_engine}" cp "${DIR}/compose/auth_proxy_htpasswd" proxy_cfg_helper:/tmp/auth_proxy_htpasswd
    "${container_engine}" cp "${cfg}" proxy_cfg_helper:/tmp/auth_proxy_alpha_config.yaml
}
if [ "${USE_PROXY}" = "true" ]; then
    createProxyCfgVolume
fi

createProxyCertsVolume() {
    "${container_engine}" volume create auth_proxy_certs
    "${container_engine}" container create --name proxy_certs_helper -v auth_proxy_certs:/certs busybox
    if [ -f "${DIR}/compose/auth_certs/certificate.pem" ] && [ -f "${DIR}/compose/auth_certs/private.key" ]; then
        chmod 640 "${DIR}/compose/auth_certs/private.key"
        "${container_engine}" cp "${DIR}/compose/auth_certs/certificate.pem" proxy_certs_helper:/certs/certificate.pem
        "${container_engine}" cp "${DIR}/compose/auth_certs/private.key" proxy_certs_helper:/certs/private.key
    else
        echo "Did you run auth_certs/generate.sh?"
        exit 2
    fi
}
if [ "${USE_PROXY}" = "true" ]; then
    createProxyCertsVolume
fi

createLocalstackCfgVolume() {
    "${container_engine}" volume create localstack_cfg
    "${container_engine}" container create --name localstack_cfg_helper -v localstack_cfg:/tmp busybox
    "${container_engine}" cp "${DIR}/compose/localstack_buckets.sh" localstack_cfg_helper:/tmp
}
if [ "${s3}" = "localstack" ]; then
    createLocalstackCfgVolume
fi

createJmxTlsCertVolume() {
    "${container_engine}" volume create jmxtls_cfg
    "${container_engine}" container create --name jmxtls_cfg_helper -v jmxtls_cfg:/truststore busybox
    if [ -d "${DIR}/truststore" ]; then
        "${container_engine}" cp "${DIR}/truststore" jmxtls_cfg_helper:/truststore
    fi
}
createJmxTlsCertVolume

createEventTemplateVolume() {
    "${container_engine}" volume create templates
    "${container_engine}" container create --name templates_helper -v templates:/templates busybox
    if [ -d "${DIR}/templates" ]; then
        "${container_engine}" cp "${DIR}/templates" templates_helper:/templates
    fi
}
createEventTemplateVolume

setupUserHosts() {
    # This requires https://github.com/figiel/hosts to work. See README.
    truncate -s 0 "${HOSTSFILE}"
    for file in "${FILES[@]}" ; do
        local hosts
        hosts="$(yq '.services.*.hostname' "${file}" | { grep -v null || test $? = 1 ; } | sed -e 's/^/localhost /')"
        echo "${hosts}" >> "${HOSTSFILE}"
    done
}
if [ "${USE_USERHOSTS}" = "true" ]; then
    setupUserHosts
fi

openBrowserTabs() {
    # TODO find a way to use 'podman wait --condition=healthy $containerId' instead of polling with curl
    set +xe
    local urls=()
    for file in "${FILES[@]}"; do
        local yaml
        yaml="$(yq '.services.* | [{"host": .hostname, "ports": .ports}]' "${file}")"
        local length
        length="$(echo "${yaml}" | yq 'length')"
        for (( i=0; i<"${length}"; i+=1 ))
        do
            local host
            local port
            if [ "${USE_USERHOSTS}" = "true" ]; then
                host="$(echo "${yaml}" | yq ".[${i}].host" | { grep -v null || test $? = 1 ; } )"
                if [ "${host}" = "auth" ]; then
                  host="localhost"
                fi
            else
                host="localhost"
            fi
            port="$(echo "${yaml}" | yq ".[${i}].ports[0]" | { grep -v null || test $? = 1 ; } | envsubst | cut -d: -f1)"
            if [ -n "${host}" ] && [ -n "${port}" ]; then
                urls+=("http://${host}:${port}")
            fi
        done
    done
    set -xe
    echo "Service URLs:" "${urls[@]}"
    for url in "${urls[@]}"; do
        (
            testSvc() {
              timeout 1s curl -s -f -o /dev/null "$1"
              local sc="$?"
              if [ "${sc}" = "0" ] || [ "${sc}" = "22" ]; then
                return 0
              else
                return "${sc}"
              fi
            }
            until testSvc "${url}"
            do
                sleep 5
            done
            xdg-open "${url}"
            echo "Opened ${url} in default browser."
        ) &
        PIDS+=($!)
    done
}
if [ "${OPEN_TABS}" = "true" ]; then
    openBrowserTabs
fi

if [ "${PULL_IMAGES}" = "true" ]; then
    IMAGES=()
    for file in "${FILES[@]}" ; do
        images="$(yq '.services.*.image' "${file}" | { grep -v null || test $? = 1; })"
        for img in ${images}; do
          IMAGES+=("$(eval echo "${img}")")
        done
    done
    ${ce} pull "${IMAGES[@]}" || true
fi

docker-compose \
    "${CMD[@]}" \
    up \
        --renew-anon-volumes \
        --remove-orphans \
        --abort-on-container-exit
