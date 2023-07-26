FROM d3fk/nfs-client:latest

RUN apk update && apk add curl

RUN mkdir -p /mnt/nfs

ENTRYPOINT ["/bin/sh", "-c"]