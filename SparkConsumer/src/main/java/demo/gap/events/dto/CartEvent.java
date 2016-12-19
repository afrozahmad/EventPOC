package demo.gap.events.dto;

import java.io.Serializable;

public class CartEvent implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Customer customer;
	private Item item;


	private String eventId;
	
	
	private String date;
	private String event;

	public String getEventId() {
		return eventId;
	}


	public void setEventId(String eventId) {
		this.eventId = eventId;
	}
	
	
	public String getEvent() {
		return event;
	}


	public void setEvent(String event) {
		this.event = event;
	}




	public Customer getCustomer() {
		return customer;
	}
	

	public void setCustomer(Customer customer) {
		this.customer = customer;
	}

	public Item getItem() {
		return item;
	}

	public void setItem(Item item) {
		this.item = item;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
	
	
	
	
}
