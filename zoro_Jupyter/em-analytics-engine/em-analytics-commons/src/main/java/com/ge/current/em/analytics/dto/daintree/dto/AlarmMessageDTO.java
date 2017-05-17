package com.ge.current.em.analytics.dto.daintree.dto;

import java.util.ArrayList;
import java.util.List;

public class AlarmMessageDTO {
	private String customer;
	private String sc;
	private List<AlarmEntry> data;
	
	public AlarmMessageDTO() {
		
	}

	public String getCustomer() {
		return customer;
	}

	public void setCustomer(String customer) {
		this.customer = customer;
	}

	public String getSc() {
		return sc;
	}

	public void setSc(String sc) {
		this.sc = sc;
	}

	public List<AlarmEntry> getData() {
		if (data == null) {
			this.data = new ArrayList<>();
		}
		return data;
	}

	public void setData(List<AlarmEntry> data) {
		this.data = data;
	}
}
