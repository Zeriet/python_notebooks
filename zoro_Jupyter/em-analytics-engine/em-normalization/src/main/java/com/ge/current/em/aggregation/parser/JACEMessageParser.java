package com.ge.current.em.aggregation.parser;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import static com.ge.current.em.aggregation.request.SiteReadingDTOContainer.SiteReadingDTOContainerBuilder.aSiteReadingDTOContainer;

import com.ge.current.em.aggregation.request.SiteReadingDTOContainer;
import com.ge.current.ie.bean.PropertiesBean;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;

import com.ge.current.em.analytics.dto.SiteReadingDTO;

/**
 * Created by 212582112 on 2/16/17.
 */
public class JACEMessageParser extends AbstractMessageParser implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(JACEMessageParser.class);

    @Override
    protected List<SiteReadingDTOContainer> getSiteReadingDTO(byte[] payload, Broadcast<Map<String, String>> gatewaySiteMapping, Broadcast<Map<String, String>> siteTimezoneMapping) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            SiteReadingDTO siteReadingDTO = objectMapper.readValue(payload, SiteReadingDTO.class);
            return Collections.singletonList(aSiteReadingDTOContainer()
                                                .withValidReading()
                                                .withSiteReading(siteReadingDTO)
                                                .build());
        } catch (Exception e) {
            return Collections.singletonList(aSiteReadingDTOContainer()
                                                .withInvalidReading()
                                                .withErrorMessage(ExceptionUtils.getStackTrace(e))
                                                .withRawSiteReading(new String(payload))
                                                .build());
        }
    }
}