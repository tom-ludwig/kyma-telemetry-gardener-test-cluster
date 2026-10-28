# Just a copy of avalanche but with alpine shell access
FROM quay.io/prometheuscommunity/avalanche:v0.7.0 AS bin
FROM alpine:latest
COPY --from=bin /bin/avalanche /usr/local/bin/avalanche
ENTRYPOINT ["/bin/sh"]