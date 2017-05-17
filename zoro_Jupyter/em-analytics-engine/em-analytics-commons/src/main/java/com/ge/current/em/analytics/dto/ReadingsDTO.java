package com.ge.current.em.analytics.dto;

/**
 * Created by Name Redacted on 11/21/2016.
 */
public class ReadingsDTO {

    private String status;

    private Object value;

    private String timeStamp;

    public ReadingsDTO() {
    }

    public ReadingsDTO(String status, Object value, String timeStamp) {
        this.status = status;
        this.value = value;
        this.timeStamp = timeStamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReadingsDTO that = (ReadingsDTO) o;

        if (!status.equals(that.status)) return false;
        if (!value.equals(that.value)) return false;
        return timeStamp.equals(that.timeStamp);

    }

    @Override
    public int hashCode() {
        int result = status.hashCode();
        result = 31 * result + value.hashCode();
        result = 31 * result + timeStamp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReadingsDTO{" +
                "status='" + status + '\'' +
                ", value=" + value +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }
}
