FROM netologygroup/mqtt-gateway:v0.9.0 as mqtt-gateway-plugin
FROM netologygroup/janus-gateway:cbf60fa as janus-conference-plugin
FROM debian:stretch

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
        libconfig-dev \
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
        make \
        git \
        vim-nox \
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
ARG JANUS_GATEWAY_COMMIT='662f1c8937df2a33306c521cab01c3acc9b3e571'

RUN set -xe \
    && JANUS_GATEWAY_BUILD_DIR=$(mktemp -d) \
    && cd "${JANUS_GATEWAY_BUILD_DIR}" \
    && git clone 'https://github.com/meetecho/janus-gateway' . \
    && git checkout "${JANUS_GATEWAY_COMMIT}" \
    && ./autogen.sh \
    && ./configure --prefix='/opt/janus' \
    && make -j $(nproc) \
    && make install \
    && make configs \
    && rm -rf "${JANUS_GATEWAY_BUILD_DIR}"

COPY --from=janus-conference-plugin /opt/janus/lib/janus/plugins/*.so /opt/janus/lib/janus/plugins/

## -----------------------------------------------------------------------------
## Configuring Janus Gateway
## -----------------------------------------------------------------------------
RUN set -xe \
    && JANUS_CONF='/opt/janus/etc/janus/janus.jcfg' \
    && perl -pi -e 's/\t#(session_timeout = ).*/\t${1}0/' "${JANUS_CONF}" \
    && perl -pi -e 's/\t#(opaqueid_in_api = ).*/\t${1}true/' "${JANUS_CONF}" \
    && JANUS_MQTT_TRANSPORT_CONF='/opt/janus/etc/janus/janus.transport.mqtt.jcfg' \
    && perl -pi -e 's/\t(enabled = ).*/\t${1}true/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && perl -pi -e 's/\t(json = ).*/\t${1}\"compact\"/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && perl -pi -e 's/\t#(client_id = ).*/\t${1}\"v1.mqtt3.payload-only\/service-agents\/alpha.janus-gateway.svc.example.org\"/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && perl -pi -e 's/\t#(retain = ).*/\t${1}false/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && perl -pi -e 's/\t(subscribe_topic = ).*/\t${1}\"agents\/alpha.janus-gateway.svc.example.org\/api\/v1\/in\/conference.svc.example.org\"/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && perl -pi -e 's/\t(publish_topic = ).*/\t${1}\"apps\/janus-gateway.svc.example.org\/api\/v1\/responses\"/' "${JANUS_MQTT_TRANSPORT_CONF}" \
    && JANUS_MQTT_EVENTS_CONF='/opt/janus/etc/janus/janus.eventhandler.mqttevh.jcfg' \
    && perl -pi -e 's/\t(enabled = ).*/\t${1}true/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t(json = ).*/\t${1}\"compact\"/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t(client_id = ).*/\t${1}\"v1.mqtt3.payload-only\/service-agents\/events-alpha.janus-gateway.svc.example.org\"/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(topic = ).*/\t${1}\"apps\/janus-gateway.svc.example.org\/api\/v1\/events\"/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(will_enabled = ).*/\t${1}true/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(will_retain = ).*/\t${1}false/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(will_qos = ).*/\t${1}1/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(connect_status = ).*/\t${1}\"{\\\"online\\\":true}\"/' "${JANUS_MQTT_EVENTS_CONF}" \
    && perl -pi -e 's/\t#(disconnect_status = ).*/\t${1}\"{\\\"online\\\":false}\"/' "${JANUS_MQTT_EVENTS_CONF}" \
    && JANUS_CONFERENCE_PLUGIN_CONF='/opt/janus/etc/janus/janus.plugin.conference.toml' \
    && printf '[recordings]\ndirectory = "/recordings"\nenabled = false\n' >> "${JANUS_CONFERENCE_PLUGIN_CONF}" \
    && printf '[uploading]\nsecret_access_key=""\naccess_key_id=""\nendpoint=""\nregion=""\n' >> "${JANUS_CONFERENCE_PLUGIN_CONF}"

## -----------------------------------------------------------------------------
## Installing VerneMQ
## -----------------------------------------------------------------------------
RUN set -xe \
    && VERNEMQ_URI='https://github.com/vernemq/vernemq/releases/download/1.7.1/vernemq-1.7.1.stretch.x86_64.deb' \
    && VERNEMQ_SHA='f705246a3390c506013921e67b2701f28b9acbd6585a318cfc537a84ed430024' \
    && curl -fSL -o vernemq.deb "${VERNEMQ_URI}" \
        && echo "${VERNEMQ_SHA} vernemq.deb" | sha1sum -c - \
        && set +e; dpkg -i vernemq.deb || apt-get -y -f --no-install-recommends install; set -e \
    && rm vernemq.deb

COPY --from=mqtt-gateway-plugin "/app" "/app"

## -----------------------------------------------------------------------------
## Configuring VerneMQ
## -----------------------------------------------------------------------------
ENV APP_AUTHN_ENABLED "0"
ENV APP_AUTHZ_ENABLED "0"
RUN set -xe \
    && VERNEMQ_ENV='/usr/lib/vernemq/lib/env.sh' \
    && perl -pi -e 's/(RUNNER_USER=).*/${1}root\n/s' "${VERNEMQ_ENV}" \
    && VERNEMQ_CONF='/etc/vernemq/vernemq.conf' \
    && perl -pi -e 's/(listener.tcp.default = ).*/${1}0.0.0.0:1883\nlistener.ws.default = 0.0.0.0:8080/g' "${VERNEMQ_CONF}" \
    && perl -pi -e 's/(plugins.vmq_passwd = ).*/${1}off/s' "${VERNEMQ_CONF}" \
    && perl -pi -e 's/(plugins.vmq_acl = ).*/${1}off/s' "${VERNEMQ_CONF}" \
    && printf "\nplugins.mqttgw = on\nplugins.mqttgw.path = /app\n" >> "${VERNEMQ_CONF}"

## -----------------------------------------------------------------------------
## Install GStreamer
## -----------------------------------------------------------------------------
RUN set -xe \
    && apt-get install -y \
        gdb libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
        gstreamer1.0-plugins-base gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav libgstrtspserver-1.0-dev
