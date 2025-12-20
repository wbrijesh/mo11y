FROM golang:1.23-alpine AS build
RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o mo11y cmd/mo11y/main.go

FROM alpine:3.20
WORKDIR /app
COPY --from=build /app/mo11y /app/mo11y
EXPOSE 8080
CMD ["./mo11y"]
