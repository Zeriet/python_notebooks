package com.ge.current.em.aggregation.mappers;

import com.datastax.driver.core.utils.UUIDs;
import com.ge.current.em.aggregation.request.EventContainer;
import com.ge.current.em.analytics.common.DateUtils;
import com.ge.current.em.analytics.dto.JaceEvent;
import com.ge.current.ie.model.nosql.MessageLog;
import org.apache.spark.api.java.function.Function;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 212582112 on 3/10/17.
 */
public class MessageLogMapper {
    public static Function<EventContainer, MessageLog> generateMessageLog = new Function<EventContainer, MessageLog>() {
        @Override
        public MessageLog call(EventContainer eventContainer) throws Exception {
            MessageLog messageLog = new MessageLog();
            Map<String, String> tags = new HashMap<>();
            tags.put("tagsrawEvent", eventContainer.getReasonPhrase());

            messageLog.setTimeBucket(DateUtils.getTimeBucket(System.currentTimeMillis()));
            messageLog.setProcessUid("Normalization");
            messageLog.setTags(tags);
            messageLog.setLogUUID(UUIDs.timeBased());
            messageLog.setErrorTs(new Date(System.currentTimeMillis()));
            messageLog.setAppKey("Normalization");
            messageLog.setCategory("");
            messageLog.setDestURI("");
            messageLog.setJobUID("");
            messageLog.setMsg(eventContainer.getRawEvent());
            messageLog.setMsgExternalURI("");
            messageLog.setMsgUID("");
            messageLog.setOriginURI("");
            messageLog.setStatus("");
            //messageLog.setMsg_ext_uri("");
            //messageLog.setMsg_sumry("");
            //messageLog.setSolr_query("");

            return messageLog;
        }
    };
}
