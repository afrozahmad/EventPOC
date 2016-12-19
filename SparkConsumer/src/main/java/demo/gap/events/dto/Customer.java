package demo.gap.events.dto;

import java.io.Serializable;

public class Customer  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String customerId;
	private String emailId;
	public String getCustomerId() {
		return customerId;
	}
	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}
	public String getEmailId() {
		return emailId;
	}
	public void setEmailId(String emailId) {
		this.emailId = emailId;
	}
	
}
