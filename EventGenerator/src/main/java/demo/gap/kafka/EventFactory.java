package demo.gap.kafka;

import java.io.IOException;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;

import demo.gap.events.CartEvent;
import demo.gap.events.Customer;
import demo.gap.events.Item;

public class EventFactory {
	
	private static String[] events=new String []{"ADD", "DELETE"};
	public static CartEvent generateAddToCartAsJson(int numCustomers) throws IOException{
		
		String custIdPRefix = "ABCDE";
		String skuPrefix = "354119012";
		
		
		
		
		Customer c = new Customer();
		Item item = new Item();
		CartEvent ce = new CartEvent();
		ce.setEvent(events[getNumbers(0,1)]);
		ce.setCustomer(c);
		ce.setDate("11/30/2016 11:20:11");
		ce.setItem(item);
		
		item.setImageUrl("http://www3.assets-gap.com/webcontent/0012/378/831/cn12378831.jpg");
		item.setItemName("Pattern crew socks (3-pack)");
		item.setQuantity(1);
		item.setSku(skuPrefix+ getNumbers(100,120));
		c.setCustomerId(custIdPRefix + getNumbers(1,3));
		c.setEmailId("a@b.com");
		
       return ce;
	}
	
	public static int getNumbers(int min, int max){
		
		Random rn = new Random();
		
		return rn.nextInt(max - min + 1) + min;
	}
	
	public static CartEvent generateDeleteItemEvent(){
		return null;
	}
	
	
	
}
