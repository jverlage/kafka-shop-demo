package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/** Liest alle Bestellungen aus dem Topic "orders" – als eigene Consumer Group. */
public class OrderConsumer {

    public static void main(String[] args) {
        // 1. Konfiguration: Wo läuft Kafka, wer bin ich (Group), wie werden Bytes wieder zu Strings?
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-demo");
        //earliest -> von Anfang an lesen, latest -> nur neue Nachrichten (über Offset)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Consumer sendet regelmäßig Hearbeat, wenn keine Rückmeldung mehr, wenn Partitionen neu verteilt
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Consumer erzeugen und Topic abonnieren
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("orders"));

            while (true) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    System.out.printf("Partition %d | Offset %d | Key %s | %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
