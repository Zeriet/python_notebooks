package com.ge.current.em.edgealarms.parser;

public class EdgeAlarmDTO {
	private String edgeDeviceID;        // ex. "Win-CCCB-9F57-782C-97D0"
	private int alarm;                  // ex. 45058
	private String time;                // ex: "2017-01-23T18:15:08.499-08:00 America/Los_Angeles"
	private String action;              // ex. "RAISED" or "CLEARED"
	
	public EdgeAlarmDTO() {}

	public String getEdgeDeviceID() {
		return edgeDeviceID;
	}

	public void setEdgeDeviceID(String edgeDeviceID) {
		this.edgeDeviceID = edgeDeviceID;
	}

	public int getAlarm() {
		return alarm;
	}

	public void setAlarm(int alarm) {
		this.alarm = alarm;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}
	
	
}
