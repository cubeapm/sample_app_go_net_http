FROM golang:1.25.5-alpine3.23

WORKDIR /app

ADD go.mod .
ADD go.sum .
RUN go mod download -x

ADD . .

RUN go build -o ./main .

CMD ["./main"]
