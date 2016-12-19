package demo.gap.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.CartEvent;

public class EventProducer {

	

	public static void main(String[] args) throws InterruptedException,
			IOException {
		
			// Thread.sleep(250);
			// pause for key input

		EventProducer eventProducer = new EventProducer();
		eventProducer.writeToKafka(args[0], args[1]);
		
		
	}

	//
	// private void aggregateEvents(CartEvents eveent){
	//
	// }
	//

	private void writeToKafka(String broker, String topic) throws InterruptedException, IOException {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", broker);
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		ObjectMapper mapper = new ObjectMapper();
		
		Aggregator aggregator = new Aggregator();
		System.out.println("Starting producer");
		for (int i = 0; i < 10; i++) {

			CartEvent event = EventFactory.generateAddToCartAsJson(3);

			String jsonInString = mapper.writeValueAsString(event);

			ProducerRecord<String, String> record = new ProducerRecord<>(
					topic, event.getCustomer().getCustomerId(),
					jsonInString);

			producer.send(record);
			
			//aggregator.add(event);
			System.out.println("Sleeping for 250ms"); 
			Thread.sleep(100);
			

		}
		//aggregator.printCart(null);
		

		producer.close();
	}


}
