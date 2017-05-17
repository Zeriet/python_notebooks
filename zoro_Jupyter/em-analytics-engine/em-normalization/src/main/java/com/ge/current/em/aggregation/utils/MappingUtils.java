package com.ge.current.em.aggregation.utils;

import com.ge.current.em.aggregation.NormalizationConstants;

import java.util.Map;
import java.util.Set;

/**
 * Created by 212582112 on 2/22/17.
 */
public class MappingUtils {
    public <T> T getResourceMappingUid(Map<String, T> resourceMapping,
                                       String assetId,
                                       T defaultValue) {
        if (assetId == null) return null;

        if(resourceMapping == null || !resourceMapping.containsKey(assetId)) return defaultValue;
        return resourceMapping.get(assetId);
    }

    public String getAssetLoadName(Set<String> segmentList,
                                   Map<String, String> segmentLoadTypeMapping) {
        String loadName = null;
        for(String segment: segmentList) {
            String loadType = getResourceMappingUid(segmentLoadTypeMapping, segment, null);

            if((loadType == null) || (loadType.split(NormalizationConstants.LOADTYPE_DELIMITER).length != 2)) continue;

            loadName = loadType.split(NormalizationConstants.LOADTYPE_DELIMITER)[1];
            break;
        }

        return loadName;
    }
}
