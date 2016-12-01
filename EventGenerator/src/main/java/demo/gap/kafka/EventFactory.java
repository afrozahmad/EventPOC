package demo.gap.kafka;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.CartEvent;
import demo.gap.events.Customer;
import demo.gap.events.Item;

public class EventFactory {
	public static String generateAddToCartAsJson(String customerId) throws IOException{
		Customer c = new Customer();
		Item item = new Item();
		CartEvent ce = new CartEvent();
		ce.setCustomer(c);
		ce.setDate("11/30/2016 11:20:11");
		ce.setItem(item);
		
		item.setImageUrl("http://www3.assets-gap.com/webcontent/0012/378/831/cn12378831.jpg");
		item.setItemName("Pattern crew socks (3-pack)");
		item.setQuantity(1);
		item.setSku("354119012001");
		c.setCustomerId(customerId);
		c.setEmailId("a@b.com");
		
        ObjectMapper mapper = new ObjectMapper();

        String jsonInString = mapper.writeValueAsString(ce);

		return jsonInString;
	}
	
	public static CartEvent generateDeleteItemEvent(){
		return null;
	}
}
