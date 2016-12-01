package demo.gap.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
	public static void main(String[] args) throws InterruptedException, IOException {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 1000; i++) {
			
			String eventAsJson = EventFactory.generateAddToCartAsJson("ABCDEF"+i);
			ProducerRecord<String, String> record = new ProducerRecord<>(
					"mytopic", "ABCDEF"+i, eventAsJson);
			producer.send(record);
			Thread.sleep(250);
		}

		producer.close();
	}
}
