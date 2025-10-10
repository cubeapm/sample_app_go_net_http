package main

import (
	"context"
	"database/sql"
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
	sqltrace "github.com/DataDog/dd-trace-go/contrib/database/sql/v2"
	mongotrace "github.com/DataDog/dd-trace-go/contrib/go.mongodb.org/mongo-driver.v2/v2/mongo"
	ddhttp "github.com/DataDog/dd-trace-go/contrib/net/http/v2"
	kafkatrace "github.com/DataDog/dd-trace-go/contrib/segmentio/kafka-go/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	redistrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/redis/go-redis.v9"
)

const kafkaTopicName = "sample_topic"

var hcl http.Client
var mysqldb *sql.DB
var rdb redis.UniversalClient
var mdb *mongo.Client
var ccn driver.Conn
var kcn *kafka.Conn
var kw *kafkatrace.KafkaWriter
var kr *kafkatrace.Reader

func main() {
	tracer.Start()
	defer tracer.Stop()
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {
	// initialize http client
	hcl = *ddhttp.WrapClient(&http.Client{})

	// initialize mysql
	mysqldb, err = sqltrace.Open("mysql", "root:root@tcp(mysql:3306)/test")
	if err != nil {
		return err
	}
	if err = mysqldb.Ping(); err != nil {
		return err
	}
	defer func() {
		_ = mysqldb.Close()
	}()

	// initialize redis
	rdb = redistrace.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	// initialize mongo
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mdbOpts := options.Client()
	mdbOpts.ApplyURI("mongodb://mongo:27017")
	mdbOpts.Monitor = mongotrace.NewMonitor()
	mdb, err = mongo.Connect(mdbOpts)
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
	// Producer
	kw = kafkatrace.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   kafkaTopicName,
	})

	// Consumer
	kr = kafkatrace.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   kafkaTopicName,
		GroupID: "my-group",
	})
	defer func() {
		if kw != nil {
			_ = kw.Close()
		}
		if kr != nil {
			_ = kr.Close()
		}
	}()

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
		wrapped := ddhttp.WrapHandler(http.HandlerFunc(handlerFunc), "go-sample-app", pattern)
		mux.Handle(pattern, wrapped)
	}

	// Register handlers.
	handleFunc("/", indexFunc)
	handleFunc("/param/{param}", paramFunc)
	handleFunc("/exception", exceptionFunc)
	handleFunc("/api", apiFunc)
	handleFunc("/mysql", mysqlFunc)
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
	span, _ := tracer.StartSpanFromContext(
		r.Context(),
		"http.handler.exception",
		tracer.ResourceName("exception"),
		tracer.SpanType("web"),
	)
	defer span.Finish()

	span.SetTag("error", true)
	span.SetTag("http.status_code", http.StatusInternalServerError)
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprint(w, "Internal Server Error")
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

func mysqlFunc(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var now string
	err := mysqldb.QueryRowContext(ctx, "SELECT NOW()").Scan(&now)
	if err != nil {
		http.Error(w, fmt.Sprintf("MySQL query error: %v", err), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "MySQL called: %s", now)
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
	span, ctx := tracer.StartSpanFromContext(
		r.Context(),
		"clickhouse.query",
		tracer.ResourceName("SELECT NOW()"),
		tracer.Tag("component", "clickhouse"),
		tracer.Tag("db.system", "clickhouse"),
	)
	defer span.Finish()
	res, err := ccn.Query(ctx, "SELECT NOW()")
	if err != nil {
		fmt.Fprintf(w, "Clickhouse query error: %v", err)
		return
	}
	span.SetTag("span.kind", "client")
	span.SetTag("db.statement", "SELECT NOW()")
	span.SetTag("db.rows", len(res.Columns()))

	fmt.Fprintf(w, "Clickhouse called: %v", res.Columns())
}

func kafkaProduceFunc(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	err := kw.WriteMessages(ctx,
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
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	msg, err := kr.ReadMessage(ctx)
	if err != nil {
		fmt.Fprintf(w, "Kafka consume error: %v", err)
		return
	}
	fmt.Fprintf(w, "Kafka consumed: %s", string(msg.Value))
}
