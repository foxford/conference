mkdir etc || true

VERNEMQ_CONTAINER_ID=$(docker run --rm -d netologygroup/mqtt-gateway:a098f05)
docker cp ${VERNEMQ_CONTAINER_ID}:/etc/vernemq/ etc/
docker rm -f ${VERNEMQ_CONTAINER_ID}

JANUS_CONTAINER_ID=$(docker run --rm -d netologygroup/janus-gateway)
docker cp ${JANUS_CONTAINER_ID}:/opt/janus/etc/janus/ etc
docker rm -f ${JANUS_CONTAINER_ID}
