package com.ge.current.em.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 212554696 on 12/6/16.
 * Deprecated - use Logger directly in drls instead of using this class.
 */
@Deprecated
public class GeneralUtil {

    public static final int LEVEL_INFO = 0;
    public static final int LEVEL_WARN = 1;
    public static final int LEVEL_ERROR = 2;

    private static final Logger LOG = LoggerFactory.getLogger(GeneralUtil.class);

    public static void log(String ruleName, int level, String... texts) {
        String message = "";
        for (String text : texts) {
            message += text + " ";
        }
        switch (level) {
        case LEVEL_INFO:
            LOG.info("[" + ruleName + "] " + message);
            break;
        case LEVEL_WARN:
            LOG.warn("[" + ruleName + "] " + message);
            break;
        case LEVEL_ERROR:
            LOG.error("[" + ruleName + "] " + message);
            break;
        default:
            LOG.info("[" + ruleName + "] " + message);
            break;
        }
    }

    public static void log(int level, String... texts) {
        String message = "";
        for (String text : texts) {
            message += text + " ";
        }
        switch (level) {
        case LEVEL_INFO:
            LOG.info(message);
            break;
        case LEVEL_WARN:
            LOG.warn(message);
            break;
        case LEVEL_ERROR:
            LOG.error(message);
            break;
        default:
            LOG.info(message);
            break;
        }
    }

    public static void log(String... texts) {
        String message = "";
        for (String text : texts) {
            message += text + " ";
        }
        LOG.info(message);
    }

}
