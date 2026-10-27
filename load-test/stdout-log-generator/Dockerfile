# Just a copy of stdout-log-generator but with shell access
FROM europe-docker.pkg.dev/kyma-project/prod/stdout-log-generator:v20261023-ed61e465 AS bin
FROM alpine:latest
COPY --from=bin /stdout-log-generator /usr/local/bin/stdout-log-generator
ENTRYPOINT ["/bin/sh"]