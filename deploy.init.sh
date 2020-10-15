#!/usr/bin/env bash

## Initializing deploy for Travis CI
if [[ "${TRAVIS}" ]]; then
    if [[ "${TRAVIS_TAG}" ]]; then
        NAMESPACE='production'
    else
        NAMESPACE='staging'
    fi
fi

if [[ ! ${NAMESPACE} ]]; then echo "NAMESPACE is required" 1>&2; exit 1; fi
if [[ ! ${GITHUB_TOKEN} ]]; then echo "GITHUB_TOKEN is required" 1>&2; exit 1; fi
BRANCH="${BRANCH:-master}"
PROJECT="conference"

SOURCE=${SOURCE:-"https://api.github.com/repos/netology-group/ulms-env/contents/k8s"}

function FILE_FROM_GITHUB() {
    local DEST_DIR="${1}"; if [[ ! "${DEST_DIR}" ]]; then echo "${FUNCNAME[0]}:DEST_DIR is required" 1>&2; exit 1; fi
    local URI="${2}"; if [[ ! "${URI}" ]]; then echo "${FUNCNAME[0]}:URI is required" 1>&2; exit 1; fi
    if [[ "${3}" != "optional" ]]; then
        local FLAGS="-fsSL"
    else
        local FLAGS="-sSL"
    fi

    mkdir -p "${DEST_DIR}"
    curl ${FLAGS} \
        -H "authorization: token ${GITHUB_TOKEN}" \
        -H 'accept: application/vnd.github.v3.raw' \
        -o "${DEST_DIR}/$(basename $URI)" \
        "${URI}?ref=${BRANCH}"
}

set -ex

FILE_FROM_GITHUB "deploy" "${SOURCE}/certs/ca-${NAMESPACE}.crt"
FILE_FROM_GITHUB "deploy" "${SOURCE}/utils/s3-docs.sh"
FILE_FROM_GITHUB "deploy" "${SOURCE}/utils/travis-run.sh"
FILE_FROM_GITHUB "deploy/k8s/base" "${SOURCE}/apps/deploy/${PROJECT}/base/kustomization.yaml"
FILE_FROM_GITHUB "deploy/k8s/base" "${SOURCE}/apps/deploy/${PROJECT}/base/${PROJECT}-headless.yaml"
FILE_FROM_GITHUB "deploy/k8s/base" "${SOURCE}/apps/deploy/${PROJECT}/base/${PROJECT}.yaml"
FILE_FROM_GITHUB "deploy/k8s/base" "${SOURCE}/apps/deploy/${PROJECT}/base/${PROJECT}-service.yaml"
FILE_FROM_GITHUB "deploy/k8s/base" "${SOURCE}/apps/deploy/${PROJECT}/base/${PROJECT}-servicemonitor.yaml"
FILE_FROM_GITHUB "deploy/k8s/base/configs" "${SOURCE}/apps/deploy/${PROJECT}/base/configs/env.ini"
FILE_FROM_GITHUB "deploy/k8s/base/patches" "${SOURCE}/apps/deploy/${PROJECT}/base/patches/update-replica-resources.yaml"
FILE_FROM_GITHUB "deploy/k8s/base/patches" "${SOURCE}/apps/deploy/${PROJECT}/base/patches/environments.yaml"
FILE_FROM_GITHUB "deploy/k8s/base/patches" "${SOURCE}/apps/deploy/${PROJECT}/base/patches/tenant-credentials.yaml"
FILE_FROM_GITHUB "deploy/k8s/overlays/ns" "${SOURCE}/apps/deploy/${PROJECT}/overlays/${NAMESPACE}/kustomization.yaml"
FILE_FROM_GITHUB "deploy/k8s/overlays/ns/configs" "${SOURCE}/apps/deploy/${PROJECT}/overlays/${NAMESPACE}/configs/App.toml"
FILE_FROM_GITHUB "deploy/k8s/overlays/ns/configs" "${SOURCE}/apps/deploy/${PROJECT}/overlays/${NAMESPACE}/configs/env.ini"
FILE_FROM_GITHUB "deploy/k8s/overlays/ns/patches" "${SOURCE}/apps/deploy/${PROJECT}/overlays/${NAMESPACE}/patches/update-replica-resources.yaml" "optional"
FILE_FROM_GITHUB "deploy/k8s/overlays/ns/patches" "${SOURCE}/apps/deploy/${PROJECT}/overlays/${NAMESPACE}/patches/tenant-credentials.yaml" "optional"

chmod u+x deploy/{s3-docs.sh,travis-run.sh}
