.PHONY: up down topics produce scenario consume dlq replay test clean

up:            ## start Kafka, Schema Registry and Kafka UI
	docker compose up -d
	@echo "Waiting for Schema Registry..."
	@until curl -sf http://localhost:8081/subjects > /dev/null; do sleep 2; done
	@echo "Stack is ready. Kafka UI: http://localhost:8080"

topics:        ## create the orders and DLQ topics
	./scripts/create_topics.sh

produce:       ## send 20 random orders
	python -m src.producer --count 20 --interval 0.3

scenario:      ## send the deterministic demo set (happy path + retry + DLQ)
	python -m src.producer --scenario --interval 0.4

consume:       ## run the consumer
	python -m src.consumer --from-beginning

dlq:           ## print everything currently in the DLQ
	python -m src.dlq_consumer

replay:        ## replay DLQ messages back onto the orders topic
	python -m src.dlq_consumer --replay

test:
	python -m pytest -q

clean:
	rm -f .aggregator_state.json
	docker compose down -v
