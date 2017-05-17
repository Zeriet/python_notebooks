
/* Too hot threshold */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-f', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','VAV','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-g', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Heater','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-h', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Ventilation Fan','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-i', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Freezer','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-j', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Cooler','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-01-asset-type-k', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-01'),'IN-PARAMETER','ASSET_TYPE','','Refrigerator','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/* Too cold threshold */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-f', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','VAV','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-g', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Heater','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-h', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Ventilation Fan','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-i', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Freezer','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-j', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Cooler','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-02-asset-type-k', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-02'),'IN-PARAMETER','ASSET_TYPE','','Refrigerator','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/*Proving Fan */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-03-asset-type-f', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-03'),'IN-PARAMETER','ASSET_TYPE','','VAV','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);


/* Short Cycling */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-04-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-04'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/* HVAC COUNT OVERRIDE */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-05-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-05'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-05-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-05'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-05-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-05'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-05-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-05'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/* LIGHTING COUNT OVERRIDE */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-06-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-06'),'IN-PARAMETER','ASSET_TYPE','','Light','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/*HVAC TIME OVERRIDE */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-07-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-07'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-07-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-07'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-07-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-07'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-07-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-07'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);



/* Zone Cooling Delta T Too High */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-09-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-09'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-09-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-09'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-09-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-09'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-09-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-09'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-09-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-09'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);


/* Zone Heating Delta T Too High */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-12-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-12'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-12-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-12'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-12-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-12'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-12-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-12'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-12-asset-type-f', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-12'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/* Zone Cooling Setpoint Unreachable*/
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-20-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-20'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-20-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-20'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-20-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-20'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-20-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-20'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-20-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-20'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/* Zone Heating Setpoint Unreachable*/
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-21-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-21'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-21-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-21'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-21-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-21'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-21-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-21'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-21-asset-type-e', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-21'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);

/*HVAC Excessive usage */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-22-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-22'),'IN-PARAMETER','ASSET_TYPE','','Split','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-22-asset-type-b', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-22'),'IN-PARAMETER','ASSET_TYPE','','Split Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-22-asset-type-c', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-22'),'IN-PARAMETER','ASSET_TYPE','','Rooftop','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-22-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-22'),'IN-PARAMETER','ASSET_TYPE','','Rooftop Heat Pump','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-22-asset-type-d', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-22'),'IN-PARAMETER','ASSET_TYPE','','AHU','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);


/*Lighting Excessive usage */
INSERT INTO ie_emsqa_emsrules.rule_property (rule_property_id,rule_property_uid,rule_id,rule_property_type,name,alias,value,value_default,data_type,uom_cde,is_editable,description,update_timestamp,entry_timestamp) VALUES (DEFAULT,'RULE-PROP-UID-23-asset-type-a', (SELECT ie_emsqa_emsrules.rule.rule_id FROM ie_emsqa_emsrules.rule WHERE ie_emsqa_emsrules.rule.rule_uid='RULE-UID-23'),'IN-PARAMETER','ASSET_TYPE','','Light','','String','N/A',DEFAULT,'ASSET_TYPE',DEFAULT,DEFAULT);




