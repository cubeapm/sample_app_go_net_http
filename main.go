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
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"go.elastic.co/apm/module/apmchiv5"
	"go.elastic.co/apm/module/apmhttp/v2"
	"go.elastic.co/apm/module/apmmongo/v2"
	"go.elastic.co/apm/v2"
)

const kafkaTopicName = "sample_topic"

var (
	// hcl     *resty.Client
	hcl     http.Client
	mysqldb *sql.DB
	rdb     *redis.Client
	mdb     *mongo.Client
	ccn     driver.Conn
	kcn     *kafka.Conn
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() (err error) {

	// hcl = resty.New()
	// hcl.SetTransport(apmhttp.WrapRoundTripper(http.DefaultTransport))

	hcl = *apmhttp.WrapClient(&http.Client{})

	// initialize mysql
	mysqldb, err = sql.Open("mysql", "root:root@tcp(mysql:3306)/test")
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
	rdb = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})
	// initialize mongo
	mdbOpts := options.Client().
		ApplyURI("mongodb://mongo:27017").
		SetMonitor(apmmongo.CommandMonitor())
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
		Handler:      InitRoutes(),
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

func InitRoutes() *chi.Mux {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	// Start parent transaction in middleware and attach to context
	// apmchiv5.Middleware() automatically sets transaction names based on route
	r.Use(apmchiv5.Middleware())

	// Register all handlers
	r.Get("/", indexFunc)
	r.Get("/param/{param}", paramFunc)
	r.Get("/exception", exceptionFunc)
	r.Get("/api", apiFunc)
	r.Get("/mysql", mysqlFunc)
	r.Get("/redis", redisFunc)
	r.Get("/mongo", mongoFunc)
	r.Get("/clickhouse", clickhouseFunc)
	r.Post("/kafka/produce", kafkaProduceFunc)
	r.Get("/kafka/consume", kafkaConsumeFunc)

	// Additional routes
	r.Get("/get", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome get"))
	})
	r.Post("/post", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("welcome post"))
	})
	r.Get("/500", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("welcome 500"))
	})

	return r
}

func indexFunc(w http.ResponseWriter, r *http.Request) {
	if _, err := io.WriteString(w, "index called"); err != nil {
		log.Printf("Write failed: %v\n", err)
	}
}

func paramFunc(w http.ResponseWriter, r *http.Request) {
	param := chi.URLParam(r, "param")
	req, err := http.NewRequestWithContext(
		r.Context(),
		http.MethodGet,
		"http://go_net_http:8000/api",
		nil,
	)

	// resp, err := hcl.R().SetContext(r.Context()).Get("http://go_net_http:8000/api")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {

		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err == nil {
			fmt.Fprintf(w, "Got api: %s", respBody)
		}

		fmt.Fprintf(w, "Got param: %s, API response: %s", param, respBody)
	} else {
		fmt.Fprintf(w, "Got param: %s, API call error: %v", param, err)
	}
}

func exceptionFunc(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := fmt.Errorf("Exception called")
	apm.CaptureError(ctx, err).Send()
	w.WriteHeader(http.StatusInternalServerError)
}

func apiFunc(w http.ResponseWriter, r *http.Request) {
	// resp, err := hcl.SetContext(r.Context()).Get("http://go_net_http:8000/")
	// if err == nil {
	// 	fmt.Fprintf(w, "Got api: %s", resp.String())
	// }
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://go_net_http:8000/", nil)
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
	span, ctx := apm.StartSpan(ctx, "SELECT users", "db.mysql.query")
	defer span.End()
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
	span, ctx := apm.StartSpan(ctx, "Redis GET", "db.redis.query")
	defer span.End()
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
	ctx := r.Context()
	span, ctx := apm.StartSpan(ctx, "Clickhouse Query", "db.clickhouse.query")
	defer span.End()

	res, err := ccn.Query(r.Context(), "SELECT NOW()")
	if err != nil {
		fmt.Fprintf(w, "Clickhouse query error: %v", err)
	}

	fmt.Fprintf(w, "Clickhouse called: %v", res.Columns())
}

func kafkaProduceFunc(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// Create a custom span for the Kafka produce operation
	span, ctx := apm.StartSpan(ctx, "Kafka Produce", "messaging.kafka.produce")
	defer span.End()

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
	ctx := r.Context()
	span, ctx := apm.StartSpan(ctx, "Kafka Consume Batch", "messaging.kafka.consume")
	defer span.End()

	kcn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_ = kcn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	fmt.Fprintf(w, "Kafka consumed")
}
