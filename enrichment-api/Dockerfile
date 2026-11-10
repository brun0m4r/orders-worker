FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /enrichment-api .

FROM gcr.io/distroless/static-debian12
COPY --from=build /enrichment-api /enrichment-api
EXPOSE 8081
ENTRYPOINT ["/enrichment-api"]
