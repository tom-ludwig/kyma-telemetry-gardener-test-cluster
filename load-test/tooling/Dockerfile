FROM golang:1.27.2 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG GIT_SHA=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags="-s -w -X github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result.ToolCommit=${GIT_SHA}" \
    -o /loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /loadtest /loadtest
ENTRYPOINT ["/loadtest"]