FROM jonashaag/webfsd:1.21

RUN apk update && apk add nano curl

COPY nodeSync.sh /entrypoint.sh
RUN chmod a+rx /entrypoint.sh
