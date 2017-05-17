package com.ge.current.em.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 212554696 on 1/6/17.
 * <p>
 * Utility class to be used in drl files. All methods should be applicable to any rule.
 */
public class RulesUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RulesUtil.class);

    public static final String TOO_HOT_SETPOINT = "Zone Cooling Setpoint Unreachable";
    public static final String TOO_COLD_SETPOINT = "Zone Heating Setpoint Unreachable";
    public static final String COOLING_DOES_NOT_WORK = "Zone Cooling Delta T Too High";
    public static final String HEATING_DOES_NOT_WORK = "Zone Heating Delta T Too High";
    public static final String TOO_HOT_THRESHOLD = "Zone Temperature Too High";
    public static final String TOO_COLD_THRESHOLD = "Zone Temperature Too Low";
    public static final String PROVING_FAN = "Supply Air Fan Failure";
    public static final String SHORT_CYCLING_THERMOSTAT_STATE = "Short Cycling";
    public static final String EXCESSIVE_HVAC_OVERRIDES_COUNT = "Excessive HVAC Overrides";
    public static final String EXCESSIVE_HVAC_OVERRIDES_TIME = "Excessive HVAC Overrides Runtime";
    public static final String EXCESSIVE_LIGHTING_OVERRIDES_COUNT = "Excessive Lighting Overrides";
    public static final String EXCESSIVE_LIGHTING_USAGE_AFTER_HOURS = "Excessive Lighting Usage After Hours";
    public static final String EXCESSIVE_HVAC_USAGE_AFTER_HOURS = "Excessive HVAC Usage After Hours";
    public static final String SUPPLY_TEMPERATURE_TOO_HIGH = "Supply Temperature Too High";
    public static final String SUPPLY_TEMPERATURE_TOO_LOW = "Supply Temperature Too Low";
    public static final String RETURN_TEMPERATURE_TOO_HIGH = "Return Temperature Too High";
    public static final String RETURN_TEMPERATURE_TOO_LOW = "Return Temperature Too Low";
    public static final String SIMULTANEOUS_HEATING_AND_COOLING = "Simultaneous Heating and Cooling";

    public static final String FAULT_CATEGORY_EQUIPMENT = "Equipment";
    public static final String FAULT_CATEGORY_COST = "Cost";
    public static final String FAULT_CATEGORY_PERFORMANCE = "Performance";
    public static final String FAULT_CATEGORY_VIOLATION = "Violation";

    public static final String CATEGORY = "Smart";

    public static boolean areTagsValid(String ruleName, List<String> tagsToBeChecked, List<String> requiredTags) {
        boolean isValid = true;
        for (String tag : requiredTags) {
            if(!tagsToBeChecked.contains(tag)) {
                isValid = false;
                LOG.info("[{}] missing field: {}", ruleName, tag);
            }
        }
        return isValid;
    }

    public static boolean areAnyTagsValid(String ruleName, List<String> tagsToBeChecked, List<String> requiredAnyTags) {
        boolean isValid = false;
        for (String tag : requiredAnyTags) {
            if(tagsToBeChecked.contains(tag)) {
                isValid = true;
            }
        }
        if (!isValid) LOG.info("[{}] Missing any of the required tags : {}", ruleName, requiredAnyTags);

        return isValid;
    }

    public static List<String> getNeededTagsFromTags(List<String> tagsMixed, List<String>  tagsToChooseFrom){
        List<String> filteredTags = new ArrayList<>();
        for(String tag: tagsMixed){
            if(tagsToChooseFrom.contains(tag)) filteredTags.add(tag);
        }
        return filteredTags;
    }

    public static String getFDSINumber(String ruleName) {
        LOG.info("rule name to get FDSI: {}", ruleName);
        String fdsiNumber = "00";
        if(ruleName == null) {
            return fdsiNumber;
        }
        switch (ruleName) {

        case TOO_HOT_THRESHOLD:
            fdsiNumber = "01";
            break;

        case TOO_COLD_THRESHOLD:
            fdsiNumber = "02";
            break;

        case PROVING_FAN:
            fdsiNumber = "03";
            break;

        case SHORT_CYCLING_THERMOSTAT_STATE:
            fdsiNumber = "04";
            break;

        case EXCESSIVE_HVAC_OVERRIDES_COUNT:
            fdsiNumber = "05";
            break;
        case EXCESSIVE_LIGHTING_OVERRIDES_COUNT:
            fdsiNumber = "06";
            break;
        case EXCESSIVE_HVAC_OVERRIDES_TIME:
            fdsiNumber = "07";
            break;

        case HEATING_DOES_NOT_WORK:
            fdsiNumber = "12";
            break;

        case COOLING_DOES_NOT_WORK:
            fdsiNumber = "09";
            break;

        case TOO_HOT_SETPOINT:
            fdsiNumber = "20";
            break;

        case TOO_COLD_SETPOINT:
            fdsiNumber = "21";
            break;
        case EXCESSIVE_HVAC_USAGE_AFTER_HOURS:
            fdsiNumber = "22";
            break;

        case EXCESSIVE_LIGHTING_USAGE_AFTER_HOURS:
            fdsiNumber = "23";
            break;

        case SUPPLY_TEMPERATURE_TOO_HIGH:
            fdsiNumber = "24";
            break;

        case SUPPLY_TEMPERATURE_TOO_LOW:
            fdsiNumber = "25";
            break;

        case RETURN_TEMPERATURE_TOO_HIGH:
            fdsiNumber = "26";
            break;

        case RETURN_TEMPERATURE_TOO_LOW:
            fdsiNumber = "27";
            break;

        case SIMULTANEOUS_HEATING_AND_COOLING:
            fdsiNumber = "28";
            break;

        default:
            break;
        }
        return fdsiNumber;
    }
}
