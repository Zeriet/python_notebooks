package com.ge.current.em.entities.analytics;

import java.io.Serializable;

import com.ge.current.ie.model.nosql.EventsHourlyLog;

public class HourlyKpiInfoBean implements Serializable {

	EventsHourlyLog currHour;
	boolean isUpdated;
	Double oldkWh;
	Double oldkW;
	Double oldMidPeakkWh;
	Double oldPeakkWh;
	Double oldOffPeakkWh;
	Double oldOccupiedkWh;
	Double oldUnOccupiedkWh;
	
	public HourlyKpiInfoBean() {
		super();
	}

    public HourlyKpiInfoBean(EventsHourlyLog currHour, boolean isUpdated, Double oldkWh, Double oldkW,
            Double oldMidPeakkWh, Double oldPeakkWh, Double oldOffPeakkWh, Double oldOccupiedkWh,
            Double oldUnOccupiedkWh) {
        this.currHour = currHour;
        this.isUpdated = isUpdated;
        this.oldkWh = oldkWh;
        this.oldkW = oldkW;
        this.oldMidPeakkWh = oldMidPeakkWh;
        this.oldPeakkWh = oldPeakkWh;
        this.oldOffPeakkWh = oldOffPeakkWh;
        this.oldOccupiedkWh = oldOccupiedkWh;
        this.oldUnOccupiedkWh = oldUnOccupiedkWh;
    }

    public EventsHourlyLog getCurrHour() {
        return currHour;
    }

    public void setCurrHour(EventsHourlyLog currHour) {
        this.currHour = currHour;
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    public void setUpdated(boolean isUpdated) {
        this.isUpdated = isUpdated;
    }

    public Double getOldkWh() {
        return oldkWh;
    }

    public void setOldkWh(Double oldkWh) {
        this.oldkWh = oldkWh;
    }

    public Double getOldkW() {
        return oldkW;
    }

    public void setOldkW(Double oldkW) {
        this.oldkW = oldkW;
    }

    public Double getOldMidPeakkWh() {
        return oldMidPeakkWh;
    }

    public void setOldMidPeakkWh(Double oldMidPeakkWh) {
        this.oldMidPeakkWh = oldMidPeakkWh;
    }

    public Double getOldPeakkWh() {
        return oldPeakkWh;
    }

    public void setOldPeakkWh(Double oldPeakkWh) {
        this.oldPeakkWh = oldPeakkWh;
    }

    public Double getOldOffPeakkWh() {
        return oldOffPeakkWh;
    }

    public void setOldOffPeakkWh(Double oldOffPeakkWh) {
        this.oldOffPeakkWh = oldOffPeakkWh;
    }

    public Double getOldOccupiedkWh() {
        return oldOccupiedkWh;
    }

    public void setOldOccupiedkWh(Double oldOccupiedkWh) {
        this.oldOccupiedkWh = oldOccupiedkWh;
    }

    public Double getOldUnOccupiedkWh() {
        return oldUnOccupiedkWh;
    }

    public void setOldUnOccupiedkWh(Double oldUnOccupiedkWh) {
        this.oldUnOccupiedkWh = oldUnOccupiedkWh;
    }

    @Override
    public String toString() {
        return "HourlyKpiInfoBean [currHour=" + currHour + ", isUpdated=" + isUpdated + ", oldkWh=" + oldkWh
                + ", oldkW=" + oldkW + ", oldMidPeakkWh=" + oldMidPeakkWh + ", oldPeakkWh=" + oldPeakkWh
                + ", oldOffPeakkWh=" + oldOffPeakkWh + ", oldOccupiedkWh=" + oldOccupiedkWh + ", oldUnOccupiedkWh="
                + oldUnOccupiedkWh + "]";
    }

	
	
}