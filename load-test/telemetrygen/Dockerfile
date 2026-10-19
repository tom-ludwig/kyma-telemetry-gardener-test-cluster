# Just a copy of telemetrygen but with shell access
FROM ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.163.0 AS bin
FROM alpine:latest
COPY --from=bin /telemetrygen /usr/local/bin/telemetrygen
ENTRYPOINT ["/bin/sh"]
