package demo.gap.events;

public class CartEvent {
	private Customer customer;
	private Item item;
	
	private String date;
	private String event;

	public String getEvent() {
		return event;
	}


	public void setEvent(String event) {
		this.event = event;
	}


	public EventType getEventType(){
		return EventType.CART;
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
