FROM alpine:3.17

RUN apk update && apk add nano curl

COPY nodeSync.sh /entrypoint.sh
RUN chmod a+rx /entrypoint.sh
