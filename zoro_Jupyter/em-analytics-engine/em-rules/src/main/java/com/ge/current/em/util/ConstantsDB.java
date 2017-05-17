package com.ge.current.em.util;

/**
 * Created by 212554696 on 12/16/16.
 */
public class ConstantsDB {

    public enum RulePropertyTable {
        RULE_PROPERTY_ID("rule_property_id"),
        RULE_PROPERTY_UID("rule_property_uid"),
        RULE_ID("rule_id"),
        RULE_PROPERTY_TYPE("rule_property_type"),
        NAME("name"),
        ALIAS("alias"),
        VALUE("value"),
        VALUE_DEFAULT("value_default"),
        DATA_TYPE("data_type"),
        UOM_CDE("uom_cde"),
        IS_EDITABLE("is_editable"),
        DESCRIPTION("description"),
        UPDATE_TIMESTAMP("update_timestamp"),
        ENTRY_TIMESTAMP("entry_timestamp");

        private String columnName;

        private static final String TABLE_NAME = "rule_property";

        RulePropertyTable(String name) {
            this.columnName = name;
        }

        public String getColumnName() {
            return  this.columnName;
        }
    }
}
