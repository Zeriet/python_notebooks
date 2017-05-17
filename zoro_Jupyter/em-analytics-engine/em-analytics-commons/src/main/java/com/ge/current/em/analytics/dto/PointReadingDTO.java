package com.ge.current.em.analytics.dto;

/**
 * Created by 212582112 on 1/24/17.
 */
public class PointReadingDTO {

    private String status;
    private Object value;
    private String timeStamp;

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

    public PointReadingDTO()
    {
    }
    public PointReadingDTO(String status, Object value, String timestamp)
    {
        this.status = status;
        this.value = value;
        this.timeStamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PointReadingDTO that = (PointReadingDTO) o;

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
        return  "\t\t\t{\n" +
                "\t\t\t\"status\": \"" + status + "\",\n" +
                "\t\t\t\"value\": \"" + value + "\",\n" +
                "\t\t\t\"timeStamp\": \"" + timeStamp + "\"\n" +
                "\t\t\t},\n";
    }

    public static final class PointReadingDTOBuilder {
        private String status;
        private Object value;
        private String timeStamp;

        private PointReadingDTOBuilder() {
        }

        public static PointReadingDTOBuilder aPointReadingDTO() {
            return new PointReadingDTOBuilder();
        }

        public PointReadingDTOBuilder withStatus(String status) {
            this.status = status;
            return this;
        }

        public PointReadingDTOBuilder withValue(Object value) {
            this.value = value;
            return this;
        }

        public PointReadingDTOBuilder withTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
            return this;
        }

        public PointReadingDTO build() {
            PointReadingDTO pointReadingDTO = new PointReadingDTO(status, value, timeStamp);
            return pointReadingDTO;
        }
    }
}