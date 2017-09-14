FROM alpine:3.4

RUN set -ex \
    && apk add --no-cache ca-certificates apache2-utils

COPY bin/registry /bin/registry
COPY cmd/registry/config-dev.yml /etc/docker/registry/config.yml

VOLUME ["/var/lib/registry"]
EXPOSE 5000
ENTRYPOINT ["registry"]
CMD ["serve", "/etc/docker/registry/config.yml"]
