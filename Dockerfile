FROM golang:1.24-alpine

WORKDIR /app

ADD go.mod .
ADD go.sum .
RUN go mod download -x

ADD . .

RUN go build -o ./main .

CMD ["./main"]
