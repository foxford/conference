#!/usr/bin/env bash

set -ex

if [[ "${LOCAL}" ]]; then

    if [[ ! ${GITHUB_TOKEN} ]]; then echo "GITHUB_TOKEN isn't specified" 1>&2; exit 1; fi

    ## Initializing deploy for a local machine

    NAMESPACE='testing'
    DOCKER_IMAGE_TAG="$(git rev-parse --short HEAD)"

else

    if [[ ! ${GITHUB_TOKEN} ]]; then echo "GITHUB_TOKEN isn't specified" 1>&2; exit 1; fi
    if [[ ! ${DOCKER_PASSWORD} ]]; then echo "DOCKER_PASSWORD isn't specified" 1>&2; exit 1; fi
    if [[ ! ${DOCKER_USERNAME} ]]; then echo "DOCKER_USERNAME isn't specified" 1>&2; exit 1; fi
    if [[ ! ${KUBE_SERVER} ]]; then echo "KUBE_SERVER isn't specified" 1>&2; exit 1; fi
    if [[ ! ${KUBE_TOKEN} ]]; then echo "KUBE_TOKEN isn't specified" 1>&2; exit 1; fi

    ## Initializing deploy for Travis CI

    if [[ "${TRAVIS_TAG}" ]]; then
        NAMESPACE='production'
        DOCKER_IMAGE_TAG="${TRAVIS_TAG}"
    else
        NAMESPACE='staging'
        DOCKER_IMAGE_TAG="$(git rev-parse --short HEAD)"
    fi

    mkdir -p ${HOME}/.local/bin
    export PATH=${HOME}/.local/bin:${PATH}

    curl -fsSLo kubectl "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" \
        && chmod +x kubectl \
        && mv kubectl "${HOME}/.local/bin"
    curl -fsSLo skaffold "https://storage.googleapis.com/skaffold/releases/v0.15.0/skaffold-linux-amd64" \
        && chmod +x skaffold \
        && mv skaffold "${HOME}/.local/bin"
    echo ${DOCKER_PASSWORD} \
        | docker login -u ${DOCKER_USERNAME} --password-stdin

    kubectl config set-cluster media --embed-certs --server ${KUBE_SERVER} --certificate-authority deploy/ca.crt
    kubectl config set-credentials travis --token ${KUBE_TOKEN}
    kubectl config set-context media --cluster media --user travis --namespace=${NAMESPACE}
    kubectl config use-context media

fi

function KUBECTL_APPLY() {
    local URI="${1}"; if [[ ! "${URI}" ]]; then echo "${FUNCNAME[0]}:URI isn't specified" 1>&2; exit 1; fi
    curl -fsSL \
        -H "authorization: token ${GITHUB_TOKEN}" \
        -H 'accept: application/vnd.github.v3.raw' \
        "${URI}" \
        | kubectl apply -f -
}

KUBECTL_APPLY "https://api.github.com/repos/netology-group/environment/contents/cluster/k8s/apps/conference/ns/${NAMESPACE}/conference-environment.yaml"
KUBECTL_APPLY "https://api.github.com/repos/netology-group/environment/contents/cluster/k8s/apps/conference/ns/${NAMESPACE}/conference-config.yaml"

IMAGE_TAG="${DOCKER_IMAGE_TAG}" skaffold run -n "${NAMESPACE}"
