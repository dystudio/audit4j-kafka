
Audit4j Kafka Handler.
==================

Kafka audit handler to capture audit records and publish them to a kafka topic.

Sample configuration:
- !org.audit4j.handler.kafka.KafkaAuditHandler
   kafka_topic: audit_topic
   kafka_host: 127.0.0.1:9092
   kafka_client_id: auditor
