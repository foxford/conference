#!/usr/bin/env bash

if [[ ! ${GITHUB_TOKEN} ]]; then echo "GITHUB_TOKEN is required" 1>&2; exit 1; fi

PROJECT="${PROJECT:-conference}"
BRANCH="${BRANCH:-master}"
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

function ADD_PROJECT() {
    local _PATH="${1}"; if [[ ! "${_PATH}" ]]; then echo "${FUNCNAME[0]}:_PATH is required" 1>&2; exit 1; fi
    local _PROJECT="${2}"; if [[ ! "${_PROJECT}" ]]; then echo "${FUNCNAME[0]}:PROJECT is required" 1>&2; exit 1; fi

    tee "${_PATH}" <<END
PROJECT=${_PROJECT}
$(cat "${_PATH}")
END
}

set -ex

if [[ -n ${NAMESPACE} ]]; then
    FILE_FROM_GITHUB "deploy" "${SOURCE}/certs/ca-${NAMESPACE}.crt"
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

    echo "In order to enable deployment NAMESPACE is required."
fi

## Get dependencies.
FILE_FROM_GITHUB "deploy" "${SOURCE}/utils/ci-install-tools.sh"

## Use the same project for build & deploy scripts.
CI_FILES=(ci-build.sh ci-deploy.sh ci-mdbook.sh)
for FILE in ${CI_FILES[@]}; do
    FILE_FROM_GITHUB "deploy" "${SOURCE}/utils/${FILE}"
    ADD_PROJECT "deploy/${FILE}" "${PROJECT}"
done

chmod u+x deploy/{ci-mdbook.sh,ci-build.sh,ci-deploy.sh,ci-install-tools.sh}
