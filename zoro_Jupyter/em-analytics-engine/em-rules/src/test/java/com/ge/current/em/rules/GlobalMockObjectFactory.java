package com.ge.current.em.rules;

import com.ge.current.em.entities.analytics.AlarmObject;
import com.ge.current.em.entities.analytics.RulesBaseFact;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by 212554696 on 4/28/17.
 */
public abstract class GlobalMockObjectFactory {

    public RulesBaseFact getFacts() {
        List<Map<String, Object>> tagsMap = new LinkedList<>();
        String assetId = "ASSET_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
        String segmentId = "SEGMENT_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
        String enterpriseId = "ENTERPRISE_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
        String siteId = "SITE_7bf8bb1b-8e46-308e-9da2-50ecc2b953d4";
        RulesBaseFact rulesBaseFact = new RulesBaseFact();
        rulesBaseFact.setAssetId(assetId);
        rulesBaseFact.setSegmentId(segmentId);
        rulesBaseFact.setEnterpriseId(enterpriseId);
        rulesBaseFact.setSiteId(siteId);
        rulesBaseFact.setAlarmObject(new AlarmObject());
        rulesBaseFact.setMeasuresAvgMap(null);
        rulesBaseFact.setParameters(createParameters());
        rulesBaseFact.setTagsMap(tagsMap);
        rulesBaseFact.setConditionMet(false);
        return rulesBaseFact;
    }

    public Map<String, Object> createMap(String tag, Object value) {
        return new HashMap<String, Object>() {{
            put(tag, value);
        }};
    }

    public Map<String, Object> createTags(String tag, Object value) {
        return new HashMap<String, Object>() {{
            put(tag, value);
        }};
    }

    abstract Map<String, Object> createParameters();
}
