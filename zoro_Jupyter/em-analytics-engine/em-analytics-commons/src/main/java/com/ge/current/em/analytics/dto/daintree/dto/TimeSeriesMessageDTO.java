/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.em.analytics.dto.daintree.dto;

import java.util.List;

public class TimeSeriesMessageDTO {
	private String customer;
	private String sc;
    private List<TimeSeriesLogDTO> data;

    public List<TimeSeriesLogDTO> getData() {
        return data;
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

    public TimeSeriesMessageDTO setData(List<TimeSeriesLogDTO> data) {
        this.data = data;
        return this;
    }

    public static final class TimeSeriesMessageDTOBuilder {
        private List<TimeSeriesLogDTO> data;

        private TimeSeriesMessageDTOBuilder() {
        }

        public static TimeSeriesMessageDTOBuilder aTimeSeriesMessageDTO() {
            return new TimeSeriesMessageDTOBuilder();
        }

        public TimeSeriesMessageDTOBuilder withData(List<TimeSeriesLogDTO> data) {
            this.data = data;
            return this;
        }

        public TimeSeriesMessageDTO build() {
            TimeSeriesMessageDTO timeSeriesMessageDTO = new TimeSeriesMessageDTO();
            timeSeriesMessageDTO.setData(data);
            return timeSeriesMessageDTO;
        }
    }
}
