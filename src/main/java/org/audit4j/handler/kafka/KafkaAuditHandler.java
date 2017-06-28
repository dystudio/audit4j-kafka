package org.audit4j.handler.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.audit4j.core.exception.HandlerException;
import org.audit4j.core.exception.InitializationException;
import org.audit4j.core.handler.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Created by rgavrilov on 6/27/17.
 */
public class KafkaAuditHandler extends Handler {

    /** The log. */
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAuditHandler.class);

    private KafkaProducer<String, String> producer = null;

    private String kafka_topic;

    private String kafka_host;

    private String kafka_client_id;


    public void handle() throws HandlerException {
        try {
           LOG.debug("sending audit message to kafka topic "+kafka_topic);
           LOG.debug(getQuery());

            producer.send(new ProducerRecord<String, String>(kafka_topic, getAuditEvent().getActor(),
                    getQuery())).get();
        } catch (InterruptedException e) {
            LOG.error(e.toString());
        } catch (ExecutionException e) {
            LOG.error(e.toString());
        }
    }

    public void init() throws InitializationException {

        Properties props = new Properties();

        props.put("bootstrap.servers", kafka_host);
        props.put("client.id", kafka_client_id);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }

    public String getKafka_topic() {
        return kafka_topic;
    }

    public void setKafka_topic(String kafka_topic) {
        this.kafka_topic = kafka_topic;
    }


    public String getKafka_host() {
        return kafka_host;
    }

    public void setKafka_host(String kafka_host) {
        this.kafka_host = kafka_host;
    }

    public String getKafka_client_id() {
        return kafka_client_id;
    }

    public void setKafka_client_id(String kafka_client_id) {
        this.kafka_client_id = kafka_client_id;
    }
}
