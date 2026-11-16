# One Dockerfile, two images:
#   --target service   HTTP API for ECS Fargate (distroless, non-root)
#   --target consumer  Lambda container image (provided:al2023 runtime)
# Both are built for linux/amd64 (deploy.sh passes --platform).

FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
RUN go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/service
RUN go build -trimpath -ldflags="-s -w" -tags lambda.norpc -o /out/bootstrap ./cmd/consumer

FROM public.ecr.aws/lambda/provided:al2023 AS consumer
COPY --from=build /out/bootstrap ${LAMBDA_RUNTIME_DIR}/bootstrap
CMD ["bootstrap"]

FROM gcr.io/distroless/static-debian12:nonroot AS service
COPY --from=build /out/service /service
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/service"]
