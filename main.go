package main

import (
	"context"
	"errors"
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
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	apiTrace "go.opentelemetry.io/otel/trace"
)

const kafkaTopicName = "sample_topic"

var tracer apiTrace.Tracer
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
	hcl = http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}

	// initialize redis
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	// Enable tracing instrumentation
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		return err
	}

	// initialize mongo
	mdbOpts := options.Client()
	// Enable tracing instrumentation
	mdbOpts.Monitor = otelmongo.NewMonitor()
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

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(ctx)
	if err != nil {
		return
	}
	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	// initialize tracer
	tracer = otel.Tracer(os.Getenv("OTEL_SERVICE_NAME"))

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
		// Configure the "http.route" for the HTTP instrumentation.
		handler := otelhttp.WithRouteTag(
			pattern, http.HandlerFunc(handlerFunc),
		)
		handler = otelhttp.NewHandler(
			handler, pattern,
			otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
				return r.Method + " " + operation
			}),
		)
		mux.Handle(pattern, handler)
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
	panic("sample panic")
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
	_, span := tracer.Start(r.Context(), "SELECT <dbname>.<tablename>", apiTrace.WithSpanKind(apiTrace.SpanKindClient))
	span.SetAttributes(
		semconv.DBSystemClickhouse,
		// semconv.DBName(""),
		semconv.DBOperation("SELECT"),
		// semconv.DBSQLTable(""),
		// semconv.DBStatement(""),
	)
	defer span.End()

	res, err := ccn.Query(r.Context(), "SELECT NOW()")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	fmt.Fprintf(w, "Clickhouse called: %v", res.Columns())
}

func kafkaProduceFunc(w http.ResponseWriter, r *http.Request) {
	_, span := tracer.Start(r.Context(), "publish "+kafkaTopicName, apiTrace.WithSpanKind(apiTrace.SpanKindProducer))
	span.SetAttributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationPublish,
		semconv.MessagingDestinationName(kafkaTopicName),
		// semconv.ServerAddress(""),
		// semconv.MessagingKafkaMessageKey(""),
		// semconv.MessagingMessageBodySize(14),
		// semconv.MessagingBatchMessageCount(3),
		// attribute.String("key", "value"), // "go.opentelemetry.io/otel/attribute"
	)
	defer span.End()

	kcn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := kcn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	fmt.Fprintf(w, "Kafka produced")
}

func kafkaConsumeFunc(w http.ResponseWriter, r *http.Request) {
	_, span := tracer.Start(r.Context(), "process "+kafkaTopicName, apiTrace.WithSpanKind(apiTrace.SpanKindConsumer))
	span.SetAttributes(
		semconv.MessagingSystemKafka,
		semconv.MessagingOperationDeliver,
		semconv.MessagingDestinationName(kafkaTopicName),
	)
	defer span.End()

	kcn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = kcn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	fmt.Fprintf(w, "Kafka consumed")
}
