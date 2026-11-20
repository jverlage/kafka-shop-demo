package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

/** Schreibt eine Bestellung in das Topic "orders". */
public class OrderProducer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        //Einstiegspunkt in das Cluster
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        // Serializer, hier vereinfacht StringSerializer: Wie wandelt Producer Key und Values in Bytes um?
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Erzeugung des Producers
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            String customer = "anna";
            String order = """
                    {"orderId":"J-%d","customerId":"%s","productId":"hoodie","quantity":1,"amount":49,"createdAt":"%s"}"""
                    .formatted(System.currentTimeMillis() % 10_000, customer, Instant.now());

            // Senden
            ProducerRecord<String, String> record = new ProducerRecord<>("orders", customer, order);
            //Send gibt Future zurück, get wartet darauf, bis Record gesendet wurde
            RecordMetadata meta = producer.send(record).get();

            System.out.printf("Gesendet an %s, Partition %d, Offset %d%n%s%n",
                    meta.topic(), meta.partition(), meta.offset(), order);
        }
    }
}
