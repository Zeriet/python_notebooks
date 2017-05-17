

/*Proving Fan */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-required-tags', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','REQUIRED_TAGS','','dischargeAirFanCmd','','String','N/A',DEFAULT,'REQUIRED_TAGS',DEFAULT,DEFAULT);

INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-required-tags', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','REQUIRED_TAGS','','coolStage1Cmd, heatStage1Cmd','','String','N/A',DEFAULT,'REQUIRED_TAGS',DEFAULT,DEFAULT);


