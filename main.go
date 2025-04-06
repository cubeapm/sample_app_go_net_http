package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const kafkaTopicName = "sample_topic"

var hcl http.Client
var rdb *redis.Client
var mdb *mongo.Client
var ccn driver.Conn
var kcn *kafka.Conn

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// initialize http client
	hcl = http.Client{}

	// initialize redis
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	// initialize mongo
	mdbOpts := options.Client()
	mdbOpts.ApplyURI("mongodb://mongo:27017")
	mdb, err = mongo.Connect(context.Background(), mdbOpts)
	if err != nil {
		return err
	}
	err = mdb.Ping(context.Background(), readpref.Primary())
	if err != nil {
		return err
	}
	defer func() {
		_ = mdb.Disconnect(context.Background())
	}()

	// initialize clickhouse
	ccn, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{"clickhouse:9000"},
	})
	if err != nil {
		return err
	}
	err = ccn.Ping(context.Background())
	if err != nil {
		return err
	}

	// initialize kafka
	kcn, err = kafka.DialLeader(context.Background(), "tcp", "kafka:9092", kafkaTopicName, 0)
	if err != nil {
		return err
	}

	// Handle SIGINT (CTRL+C) gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Start HTTP server.
	srv := &http.Server{
		Addr:         ":8000",
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
		ReadTimeout:  time.Second,
		WriteTimeout: 10 * time.Second,
		Handler:      newHTTPHandler(),
	}
	srvErr := make(chan error, 1)
	go func() {
		srvErr <- srv.ListenAndServe()
	}()

	// Wait for interruption.
	select {
	case err = <-srvErr:
		// Error when starting HTTP server.
		return
	case <-ctx.Done():
		// Wait for first CTRL+C.
		// Stop receiving signal notifications as soon as possible.
		stop()
	}

	// When Shutdown is called, ListenAndServe immediately returns ErrServerClosed.
	err = srv.Shutdown(context.Background())
	return
}

func newHTTPHandler() http.Handler {
	mux := http.NewServeMux()

	// handleFunc is a replacement for mux.HandleFunc
	// which enriches the handler's HTTP instrumentation.
	handleFunc := func(pattern string, handlerFunc func(http.ResponseWriter, *http.Request)) {
		mux.HandleFunc(pattern, handlerFunc)
	}

	// Register handlers.
	handleFunc("/", indexFunc)
	handleFunc("/param/{param}", paramFunc)
	handleFunc("/exception", exceptionFunc)
	handleFunc("/api", apiFunc)
	handleFunc("/redis", redisFunc)
	handleFunc("/mongo", mongoFunc)
	handleFunc("/clickhouse", clickhouseFunc)
	handleFunc("/kafka/produce", kafkaProduceFunc)
	handleFunc("/kafka/consume", kafkaConsumeFunc)

	return mux
}

func indexFunc(w http.ResponseWriter, r *http.Request) {
	if _, err := io.WriteString(w, "index called"); err != nil {
		log.Printf("Write failed: %v\n", err)
	}
}

func paramFunc(w http.ResponseWriter, r *http.Request) {
	param := r.PathValue("param")
	fmt.Fprintf(w, "Got param: %s", param)
}

func exceptionFunc(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusInternalServerError)
}

func apiFunc(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://localhost:8000/", nil)
	resp, err := hcl.Do(req)
	if err == nil {
		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err == nil {
			fmt.Fprintf(w, "Got api: %s", respBody)
		}
	}
}

func redisFunc(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	val, err := rdb.Get(ctx, "key").Result()
	if err != nil {
		io.WriteString(w, err.Error())
	} else {
		fmt.Fprintf(w, "Redis called: %s", val)
	}
}

func mongoFunc(w http.ResponseWriter, r *http.Request) {
	collection := mdb.Database("sample_db").Collection("sampleCollection")
	_ = collection.FindOne(r.Context(), bson.D{{Key: "name", Value: "dummy"}})
	fmt.Fprintf(w, "Mongo called")
}

func clickhouseFunc(w http.ResponseWriter, r *http.Request) {

	res, err := ccn.Query(r.Context(), "SELECT NOW()")
	if err != nil {
		fmt.Fprintf(w, "Clickhouse query error: %v", err)
	}

	fmt.Fprintf(w, "Clickhouse called: %v", res.Columns())
}

func kafkaProduceFunc(w http.ResponseWriter, r *http.Request) {

	kcn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := kcn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		fmt.Fprintf(w, "Kafka produce error: %v", err)
	}

	fmt.Fprintf(w, "Kafka produced")
}

func kafkaConsumeFunc(w http.ResponseWriter, r *http.Request) {

	kcn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = kcn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	fmt.Fprintf(w, "Kafka consumed")
}
