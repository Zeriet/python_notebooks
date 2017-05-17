package com.ge.current.em.analytics.dto;

import java.util.List;

/**
 * Created by 212582112 on 1/24/17.
 */
public class PointDTO {
    private String pointName;
    private String curStatus;
    private Object curValue;
    private String pointType;
    private List<PointReadingDTO> pointReadings;

    public String getPointName() { return pointName; }

    public void setPointName(String pointName) { this.pointName = pointName; }

    public String getCurStatus() { return curStatus; }

    public void setCurStatus(String curStatus) { this.curStatus = curStatus; }

    public Object getCurValue() { return curValue; }

    public void setCurValue(Object curValue) { this.curValue = curValue; }

    public String getPointType() { return pointType; }

    public void setPointType(String pointType) { this.pointType = pointType; }

    public List<PointReadingDTO> getPointReadings() { return pointReadings; }

    public void setPointReadings(List<PointReadingDTO> pointReadings) { this.pointReadings = pointReadings; }

    public int deepSize() {
        return pointReadings.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PointDTO pointDTO = (PointDTO) o;

        if (pointName != null ? !pointName.equals(pointDTO.pointName) : pointDTO.pointName != null) return false;
        if (curStatus != null ? !curStatus.equals(pointDTO.curStatus) : pointDTO.curStatus != null) return false;
        if (curValue != null ? !curValue.equals(pointDTO.curValue) : pointDTO.curValue != null) return false;
        if (pointType != null ? !pointType.equals(pointDTO.pointType) : pointDTO.pointType != null) return false;
        return pointReadings != null ? pointReadings.equals(pointDTO.pointReadings) : pointDTO.pointReadings == null;

    }

    @Override
    public int hashCode() {
        int result = pointName != null ? pointName.hashCode() : 0;
        result = 31 * result + (curStatus != null ? curStatus.hashCode() : 0);
        result = 31 * result + (curValue != null ? curValue.hashCode() : 0);
        result = 31 * result + (pointType != null ? pointType.hashCode() : 0);
        result = 31 * result + (pointReadings != null ? pointReadings.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return  "{\n\t\t\"pointName\": \"" + pointName + "\",\n" +
                "\t\t\"curStatus\": \"" + curStatus + "\",\n" +
                "\t\t\"curValue\": \"" + curValue + "\",\n" +
                "\t\t\"pointType\": \"" + pointType + "\",\n" +
                "\t\t\"pointReadings\": [\n" + pointReadings +
                "\t\t\n],\n}";
    }

    public static final class PointDTOBuilder {
        private String pointName;
        private String curStatus;
        private Object curValue;
        private String pointType;
        private List<PointReadingDTO> pointReadings;

        private PointDTOBuilder() {
        }

        public static PointDTOBuilder aPointDTO() {
            return new PointDTOBuilder();
        }

        public PointDTOBuilder withPointName(String pointName) {
            this.pointName = pointName;
            return this;
        }

        public PointDTOBuilder withCurStatus(String curStatus) {
            this.curStatus = curStatus;
            return this;
        }

        public PointDTOBuilder withCurValue(Object curValue) {
            this.curValue = curValue;
            return this;
        }

        public PointDTOBuilder withPointType(String pointType) {
            this.pointType = pointType;
            return this;
        }

        public PointDTOBuilder withPointReadings(List<PointReadingDTO> pointReadings) {
            this.pointReadings = pointReadings;
            return this;
        }

        public PointDTO build() {
            PointDTO pointDTO = new PointDTO();
            pointDTO.setPointName(pointName);
            pointDTO.setCurStatus(curStatus);
            pointDTO.setCurValue(curValue);
            pointDTO.setPointType(pointType);
            pointDTO.setPointReadings(pointReadings);
            return pointDTO;
        }
    }
}