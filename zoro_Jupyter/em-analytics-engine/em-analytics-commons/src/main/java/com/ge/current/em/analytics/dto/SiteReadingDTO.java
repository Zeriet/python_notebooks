package com.ge.current.em.analytics.dto;

import java.util.List;

/**
 * Created by 212582112 on 1/24/17.
 */
public class SiteReadingDTO {
    private String edgeDeviceId;
    private String edgeDeviceType;
    private String reportTime;
    private int reportingIntervalInSeconds;
    private List<PointDTO> points;

    public String getEdgeDeviceId() { return edgeDeviceId; }

    public void setEdgeDeviceId(String edgeDeviceId) { this.edgeDeviceId = edgeDeviceId; }

    public String getEdgeDeviceType() { return edgeDeviceType; }

    public void setEdgeDeviceType(String edgeDeviceType) { this.edgeDeviceType = edgeDeviceType; }

    public String getReportTime() { return reportTime; }

    public void setReportTime(String reportTime) { this.reportTime = reportTime; }

    public int getReportingIntervalInSeconds() { return reportingIntervalInSeconds; }

    public void setReportingIntervalInSeconds(int reportingIntervalInSeconds) { this.reportingIntervalInSeconds = reportingIntervalInSeconds; }

    public List<PointDTO> getPoints() {return points; }

    public void setPoints(List<PointDTO> points) { this.points = points; }

    public int deepSize() {
        int size = 0;
        for (PointDTO pt : points) {
            size += pt.deepSize();
        }
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SiteReadingDTO that = (SiteReadingDTO) o;

        if (reportingIntervalInSeconds != that.reportingIntervalInSeconds) return false;
        if (edgeDeviceId != null ? !edgeDeviceId.equals(that.edgeDeviceId) : that.edgeDeviceId != null) return false;
        if (edgeDeviceType != null ? !edgeDeviceType.equals(that.edgeDeviceType) : that.edgeDeviceType != null)
            return false;
        if (reportTime != null ? !reportTime.equals(that.reportTime) : that.reportTime != null) return false;
        return points != null ? points.equals(that.points) : that.points == null;

    }

    @Override
    public int hashCode() {
        int result = edgeDeviceId != null ? edgeDeviceId.hashCode() : 0;
        result = 31 * result + (edgeDeviceType != null ? edgeDeviceType.hashCode() : 0);
        result = 31 * result + (reportTime != null ? reportTime.hashCode() : 0);
        result = 31 * result + reportingIntervalInSeconds;
        result = 31 * result + (points != null ? points.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{\n\"SiteReadingDTO\": {\n" +
                "\t\"edgeDeviceId\": \"" + edgeDeviceId + "\",\n" +
                "\t\"edgeDeviceType\": \"" + edgeDeviceType + "\",\n" +
                "\t\"reportTime\": \"" + reportTime + "\",\n" +
                "\t\"reportingIntervalInSeconds\": \"" + reportingIntervalInSeconds + "\",\n" +
                "\t\"points\": [\n" + points +
                "\t]\n}\n" +
                '}';
    }

    public static final class SiteReadingDTOBuilder {
        private String edgeDeviceId;
        private String edgeDeviceType;
        private String reportTime;
        private int reportingIntervalInSeconds;
        private List<PointDTO> points;

        private SiteReadingDTOBuilder() {
        }

        public static SiteReadingDTOBuilder aSiteReadingDTO() {
            return new SiteReadingDTOBuilder();
        }

        public SiteReadingDTOBuilder withEdgeDeviceId(String edgeDeviceId) {
            this.edgeDeviceId = edgeDeviceId;
            return this;
        }

        public SiteReadingDTOBuilder withEdgeDeviceType(String edgeDeviceType) {
            this.edgeDeviceType = edgeDeviceType;
            return this;
        }

        public SiteReadingDTOBuilder withReportTime(String reportTime) {
            this.reportTime = reportTime;
            return this;
        }

        public SiteReadingDTOBuilder withReportingIntervalInSeconds(int reportingIntervalInSeconds) {
            this.reportingIntervalInSeconds = reportingIntervalInSeconds;
            return this;
        }

        public SiteReadingDTOBuilder withPoints(List<PointDTO> points) {
            this.points = points;
            return this;
        }

        public SiteReadingDTO build() {
            SiteReadingDTO siteReadingDTO = new SiteReadingDTO();
            siteReadingDTO.setEdgeDeviceId(edgeDeviceId);
            siteReadingDTO.setEdgeDeviceType(edgeDeviceType);
            siteReadingDTO.setReportTime(reportTime);
            siteReadingDTO.setReportingIntervalInSeconds(reportingIntervalInSeconds);
            siteReadingDTO.setPoints(points);
            return siteReadingDTO;
        }
    }
}