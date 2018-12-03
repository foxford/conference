FROM netologygroup/mqtt-gateway:a098f05 as mqtt-gateway-plugin
FROM ubuntu:18.04

ENV DEBIAN_FRONTEND noninteractive

## -----------------------------------------------------------------------------
## Installing dependencies
## -----------------------------------------------------------------------------
RUN set -xe \
    && apt-get update \
    && apt-get -y --no-install-recommends install \
        apt-transport-https \
        ca-certificates \
        curl \
        less \
        libmicrohttpd-dev \
        libjansson-dev \
        libnice-dev \
        libcurl4-openssl-dev \
        libsofia-sip-ua-dev \
        libopus-dev \
        libogg-dev \
        libwebsockets-dev \
        libsrtp2-dev \
        gengetopt \
        libtool \
        automake \
        cmake \
        git \
    && PAHO_MQTT_BUILD_DIR=$(mktemp -d) \
        && PAHO_MQTT_VERSION='1.3.0' \
        && cd "${PAHO_MQTT_BUILD_DIR}" \
        && git clone "https://github.com/eclipse/paho.mqtt.c.git" . \
        && git checkout "v${PAHO_MQTT_VERSION}" \
        && make \
        && make install

## -----------------------------------------------------------------------------
## Installing Janus Gateway
## -----------------------------------------------------------------------------
ARG JANUS_GATEWAY_VERSION=0.4.5

RUN set -xe \
    && JANUS_GATEWAY_BUILD_DIR=$(mktemp -d) \
    && cd "${JANUS_GATEWAY_BUILD_DIR}" \
    && git clone 'https://github.com/meetecho/janus-gateway' . \
    && git checkout "v${JANUS_GATEWAY_VERSION}" \
    && ./autogen.sh \
    && ./configure --prefix=/opt/janus \
    && make -j $(nproc) \
    && make install \
    && make configs \
    && rm -rf "${JANUS_GATEWAY_BUILD_DIR}"

## -----------------------------------------------------------------------------
## Configuring Janus Gateway
## -----------------------------------------------------------------------------
RUN set -xe \
    && JANUS_CONF='/opt/janus/etc/janus/janus.cfg' \
    && perl -pi -e 's/(debug_level = ).*/${1}6/' "${JANUS_CONF}" \
    && JANUS_MQTT_CONF='/opt/janus/etc/janus/janus.transport.mqtt.cfg' \
    && perl -pi -e 's/(enable = ).*/${1}yes/' "${JANUS_MQTT_CONF}" \
    && perl -pi -e 's/;(client_id = ).*/${1}mqtt3v1\/agents\/jgcp-1.00000000-0000-1017-a000-000000000000.example.org/' "${JANUS_MQTT_CONF}"

## -----------------------------------------------------------------------------
## Installing VerneMQ
## -----------------------------------------------------------------------------
RUN set -xe \
    && VERNEMQ_URI='https://bintray.com/artifact/download/erlio/vernemq/deb/bionic/vernemq_1.6.1-1_amd64.deb' \
    && VERNEMQ_SHA='2aec003035996928d9d4bb7c40221acda36fcbf6a670671afd6d42028083be18' \
    && curl -fSL -o vernemq.deb "${VERNEMQ_URI}" \
        && echo "${VERNEMQ_SHA} vernemq.deb" | sha1sum -c - \
        && set +e; dpkg -i vernemq.deb || apt-get -y -f --no-install-recommends install; set -e \
    && rm vernemq.deb

COPY --from=mqtt-gateway-plugin "/app" "/app"

## -----------------------------------------------------------------------------
## Configuring VerneMQ
## -----------------------------------------------------------------------------
RUN set -xe \
    && VERNEMQ_ENV='/usr/lib/vernemq/lib/env.sh' \
    && perl -pi -e 's/(RUNNER_USER=).*/${1}root\n/s' "${VERNEMQ_ENV}" \
    && VERNEMQ_CONF='/etc/vernemq/vernemq.conf' \
    && perl -pi -e 's/(listener.tcp.default = ).*/${1}0.0.0.0:1883\nlistener.ws.default = 0.0.0.0:8080/g' "${VERNEMQ_CONF}" \
    && perl -pi -e 's/(plugins.vmq_passwd = ).*/${1}off/s' "${VERNEMQ_CONF}" \
    && perl -pi -e 's/(plugins.vmq_acl = ).*/${1}off/s' "${VERNEMQ_CONF}" \
    && printf "\nplugins.mqttgw = on\nplugins.mqttgw.path = /app\n" >> "${VERNEMQ_CONF}"