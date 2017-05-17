package com.ge.current.em.analytics.dto;

import java.util.List;

/**
 * Created by Name Redacted on 11/21/2016.
 */
public class EquipDTO {
    private String equipName;
    private String externalRefID;
    private String reportTime;
    private int frequency;
    private List<PointDTO> points;

    public EquipDTO() {
    }

    public EquipDTO(String equipName, String externalRefID, String reportTime, int frequency, List<PointDTO> points) {
        this.equipName = equipName;
        this.externalRefID = externalRefID;
        this.reportTime = reportTime;
        this.frequency = frequency;
        this.points = points;
    }

    public String getEquipName() { return equipName; }

    public void setEquipName(String equipName) { this.equipName = equipName; }

    public String getReportTime() { return reportTime; }

    public void setReportTime(String reportTime) { this.reportTime = reportTime; }

    public int getFrequency() { return frequency; }

    public void setFrequency(int frequency) { this.frequency = frequency; }

    public List<PointDTO> getPoints() { return points; }

    public void setPoints(List<PointDTO> points) { this.points = points; }

    public String getExternalRefID() { return externalRefID; }

    public void setExternalRefID(String externalRefID) { this.externalRefID = externalRefID; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EquipDTO equipDTO = (EquipDTO) o;

        if (frequency != equipDTO.frequency) return false;
        if (!equipName.equals(equipDTO.equipName)) return false;
        if (!externalRefID.equals(equipDTO.externalRefID)) return false;
        if (!reportTime.equals(equipDTO.reportTime)) return false;

        return points != null ? points.equals(equipDTO.points) : equipDTO.points == null;
    }

    @Override
    public int hashCode() {
        int result = equipName.hashCode();
        result = 31 * result + externalRefID.hashCode();
        result = 31 * result + reportTime.hashCode();
        result = 31 * result + frequency;
        result = 31 * result + (points != null ? points.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return "EquipDTO{" +
                "equipName='" + equipName + '\'' +
                ", externalRefID='" + externalRefID + '\'' +
                ", reportTime='" + reportTime + '\'' +
                ", frequency=" + frequency +
                ", points=" + points +
                '}';
    }
}
