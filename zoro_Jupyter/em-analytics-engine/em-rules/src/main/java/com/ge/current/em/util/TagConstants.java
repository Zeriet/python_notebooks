
package com.ge.current.em.util;

public class TagConstants {

	public enum Tags {

		DISCHARGE_AIR_FAN_SENSOR("dischargeAirFanSensor"),
		DISCHARGE_AIR_TEMP_SENSOR("dischargeAirTempSensor"),
		INDOOR_LIGHTING_STATUS_ON_RUNTIME("lightCmd_on_runtime"), // used in Excessive Lighting Usage After Hours
		HVAC_STATE_OVERRIDE("hvacOverrideStatus_true"),  // used in Excessive HVAC Overrides - Count
		HVAC_STATE_OVERRIDE_TRUE_RUNTIME("hvacOverrideStatus_true_runtime"),  // used in Excessive HVAC Overrides - time
		LIGHTING_STATE_OVERRIDE("lightOverrideStatus_true"), // used in Excessive Lighting Overrides - Count
		DISCHARGE_AIR_FAN_CURRENT_SENSOR("dischargeAirFanCurrentSensor"), // rule: Proving Fan
		DISCHARGE_AIR_FAN_CMD("dischargeAirFanCmd"), // rule: Proving Fan
		ZONE_AIR_TEMP_EFFECTIVE_SP("zoneAirTempEffectiveSp"),
		ZONE_AIR_TEMP_EFFECTIVE_SP_COOLING("zoneAirTempCoolingSp"), // rule: Too Hot - Setpoint
		ZONE_AIR_TEMP_EFFECTIVE_SP_HEATING("zoneAirTempHeatingSp"), // rule: Too Cold - Setpoint
		AHU_HEAT_STAGE_1_ON_RUNTIME("heatStage1Cmd_on_runtime"),
		AHU_COOL_STAGE_1_ON_RUNTIME("coolStage1Cmd_on_runtime"),
		RTU_COOL_STAGE_1_ON_RUNTIME("coolStage1Cmd_on_runtime"), // used in Excessive HVAC Usage After Hours and Cooling does not work
		RTU_HEAT_STAGE_1_ON_RUNTIME("heatStage1Cmd_on_runtime"), // used in Excessive HVAC Usage After Hours and Heatin does not work

		RTU_HEAT_STAGE1("heatStage1Cmd"),
	    RTU_COOL_STAGE1("coolStage1Cmd"),
		AHU_HEAT_STAGE_1("heatStage1Cmd"),
		AHU_HEAT_STAGE_2("heatStage2Cmd"),
		AHU_HEAT_STAGE_3("heatStage3Cmd"),
		AHU_HEAT_STAGE_4("heatStage4cmd"),
		AHU_COOL_STAGE_1("coolStage1Cmd"),
		AHU_COOL_STAGE_2("coolStage2Cmd"),
		AHU_COOL_STAGE_3("coolStage3Cmd"),
		AHU_COOL_STAGE_4("coolStage4Cmd");
		private String tagName;

		Tags(String tagName) {this.tagName = tagName;}

		public String getTagName() {return this.tagName;}
	}

	public enum Measures {

		ZONE_AIR_TEMP_SENSOR("zoneAirTempSensor"),
		DISCHARGE_AIR_FAN_CMD("dischargeAirFanCmd"),
		RETURN_AIR_TEMP_SENSOR("returnAirTempSensor"), // used in Return Temperature Too High, Too Low
		EVENT_TS("event_ts");

		private String measureName;

		Measures(String measureName) {this.measureName = measureName;}

		public  String getMeasureName() {return  this.measureName;}
	}

	public static final String ON_COMMAND_STATUS = "on";
	public static final String OFF_COMMAND_STATUS = "off";
	public static final int ON_OFF_WINDOW_LENGTH = 3;

	public enum Parameters {

		COMFORT_THRESHOLD("comfortThreshold"), // Too Hot, Too Cold threshold
		CURRENT_THRESHOLD("currentThreshold"), //rule: Proving Fan
		SETPOINT_OFFSET("setpointOffset"), //rule: Too Hot, Too Cold setpoint
		USER_THRESHOLD("user_Threshold"),
		TEMPERATURE_THRESHOLD("temperatureThreshold"),
		MIN_ITD("minITD"),
		RUNTIME_THRESHOLD("duration"),
		SEVERITY("severity"),
		DURATION("duration"),
		TEMP_HOLD_DURATION("temp_hold_duration"); // used as duration threshold

		private String parameterName;

		Parameters(String parameterName) {this.parameterName = parameterName;}

		public String getParameterName() {return this.parameterName;}
	}
}
