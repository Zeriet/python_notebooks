package com.ge.current.em.entities.analytics;

import java.io.Serializable;

public class AHUBean implements Serializable {
	
	private Double zoneAirTemperatureSensor;
	private Double zoneAirTemperatureSP;
	private Double deadBand;
	private Double ITD;
	
	public Double getZoneAirTemperatureSensor() {
		return zoneAirTemperatureSensor;
	}
	public void setZoneAirTemperatureSensor(Double zoneAirTemperatureSensor) {
		this.zoneAirTemperatureSensor = zoneAirTemperatureSensor;
	}
	public Double getZoneAirTemperatureSP() {
		return zoneAirTemperatureSP;
	}
	public void setZoneAirTemperatureSP(Double zoneAirTemperatureSP) {
		this.zoneAirTemperatureSP = zoneAirTemperatureSP;
	}
	public Double getDeadBand() {
		return deadBand;
	}
	public void setDeadBand(Double deadBand) {
		this.deadBand = deadBand;
	}
	public Double getITD() {
		return ITD;
	}
	public void setITD(Double iTD) {
		ITD = iTD;
	}

}
