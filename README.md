# OpenTelemetry Instrumentation

This branch contains code for OpenTelemetry instrumentation.

Hitting an API endpoint will generate the corresponding traces. Traces are printed to console (where docker compose is running) by default. If you want to send traces to a backend tool, comment out the `OTEL_LOG_LEVEL` line and uncomment the `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` line in [docker-compose.yml](docker-compose.yml).

Refer the project README below for more details.

---

# Go net/http Instrumentation

This is a sample app to demonstrate how to instrument Go app using net/http with **Datadog**, **Elastic**, **New Relic** and **OpenTelemetry**. It contains source code for the Go app which interacts with various services like Redis, MySQL, MongoDB, Kafka, ClickHouse, etc. to demonstrate tracing for these services. This repository has a docker compose file to set up all these services conveniently.

The code is organized into multiple branches. The main branch has the Go net/http app without any instrumentation. Other branches then build upon the main branch to add specific instrumentations as below:

| Branch                                                                                         | Instrumentation | Code changes for instrumentation                                                                                |
| ---------------------------------------------------------------------------------------------- | --------------- | --------------------------------------------------------------------------------------------------------------- |
| [main](https://github.com/cubeapm/sample_app_go_net_http/tree/main)         | None            | -                                                                                                               |
| [datadog](https://github.com/cubeapm/sample_app_go_net_http/tree/datadog) | Datadog       | [main...datadog](https://github.com/cubeapm/sample_app_go_net_http/compare/main...datadog) |
| [elastic](https://github.com/cubeapm/sample_app_go_net_http/tree/elastic)         | Elastic   | [main...elastic](https://github.com/cubeapm/sample_app_go_net_http/compare/main...elastic)         |
| [newrelic](https://github.com/cubeapm/sample_app_go_net_http/tree/newrelic) | New Relic       | [main...newrelic](https://github.com/cubeapm/sample_app_go_net_http/compare/main...newrelic) |
| [otel](https://github.com/cubeapm/sample_app_go_net_http/tree/otel)         | OpenTelemetry   | [main...otel](https://github.com/cubeapm/sample_app_go_net_http/compare/main...otel)         |

## Setup

Clone this repository and go to the project directory. Then run the following commands

```
docker compose up --build
```

Go app will now be available at `http://localhost:8000`.

The app has various API endpoints to demonstrate OpenTelemetry integrations with Redis, MySQL, MongoDB, Kafka, ClickHouse, etc. Check out [main.go](main.go) for the list of API endpoints.

# Contributing

Please feel free to raise PR for any enhancements - additional service integrations, library version updates, documentation updates, etc.
