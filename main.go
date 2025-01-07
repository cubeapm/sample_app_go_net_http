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

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var hcl http.Client
var rdb *redis.Client
var mdb *mongo.Client

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
		panic(err)
	}

	// initialize mongo
	mdbOpts := options.Client()
	// Enable tracing instrumentation
	mdbOpts.Monitor = otelmongo.NewMonitor()
	mdbOpts.ApplyURI("mongodb://mongo:27017")
	mdb, _ = mongo.Connect(context.Background(), mdbOpts)
	_ = mdb.Ping(context.Background(), readpref.Primary())
	defer func() {
		_ = mdb.Disconnect(context.Background())
	}()

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
