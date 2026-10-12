FROM golang:1.22 AS build
WORKDIR /src
COPY . .
# no dependencies to download. static binary, no cgo.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /idle-reaper ./cmd/idle-reaper

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /idle-reaper /idle-reaper
USER nonroot:nonroot
ENTRYPOINT ["/idle-reaper"]
