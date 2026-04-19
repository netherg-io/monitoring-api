FROM golang:1.23-alpine AS build
WORKDIR /src
ENV GOTOOLCHAIN=auto
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/monitoring-api .

FROM gcr.io/distroless/static-debian12:latest
COPY --from=build /out/monitoring-api /monitoring-api
EXPOSE 8080
ENTRYPOINT ["/monitoring-api"]
