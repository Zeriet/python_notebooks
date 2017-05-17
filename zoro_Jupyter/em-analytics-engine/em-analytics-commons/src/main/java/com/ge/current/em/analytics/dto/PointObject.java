package com.ge.current.em.analytics.dto;

import java.io.Serializable;

public class PointObject implements Serializable {

    private static final long serialVersionUID = 1L;
    private String assetSourceKey;
    private String haystackName;

    public PointObject(String assetSourceKey, String haystackName) {
        this.assetSourceKey= assetSourceKey;
        this.haystackName= haystackName;
    }

    public String getAssetSourceKey() {
        return assetSourceKey;
    }

    public void setAssetSourceKey(String assetSourceKey) {
        this.assetSourceKey= assetSourceKey;
    }

    public String getHaystackName() {
        return haystackName;
    }

    public void setHaystackName(String haystackName) {
        this.haystackName= haystackName;
    }

    public String toString() {
        return " *** assetSourceKey = " + assetSourceKey + " *** haystackName = " + haystackName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PointObject that = (PointObject) o;

        if (assetSourceKey != null ? !assetSourceKey.equals(that.assetSourceKey) : that.assetSourceKey != null) {
            return false;
        }
        return haystackName != null ? haystackName.equals(that.haystackName) : that.haystackName == null;

    }

    @Override
    public int hashCode() {
        int result = assetSourceKey != null ? assetSourceKey.hashCode() : 0;
        result = 31 * result + (haystackName != null ? haystackName.hashCode() : 0);
        return result;
    }
}