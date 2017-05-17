package com.ge.current.em.analytics.dto.daintree.attribute;

import java.util.function.Function;

import com.daintreenetworks.haystack.commons.model.types.status.OverrideState;
import com.daintreenetworks.haystack.commons.model.types.status.ZoneLightState;
import com.daintreenetworks.haystack.commons.model.types.status.ZoneOccupancyState;
import com.ge.current.em.analytics.dto.daintree.dto.EventEntry;

public enum Attribute implements Function<EventEntry, PointValue> {
	UNKNOWN("UNKNOWN", Unit.NONE, Value.NONE) {
		public PointValue apply(EventEntry t) {
			return new PointValue(t);
		}
	},

	// Lighting zone related
	LIGHT_STATE("LIGHT_STATE", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Enum, ZoneLightState.ON.name().equals(t.getTextValue()) ? "ON" : "OFF");
		}
	},

	LIGHT_STATE_OVR("LIGHT_STATE_OVR", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	LIGHT_INTENSITY("LIGHT_INTENSITY", Unit.PERCENTAGE, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getIntValue());
		}
	},

	LIGHT_INTENSITY_OVR("LIGHT_INTENSITY_OVR", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	OCCUPANCY("OCCUPANCY", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					ZoneOccupancyState.OCCUPIED.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	OCCUPANCY_DEVICE("OCCUPANCY_DEVICE", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					ZoneOccupancyState.OCCUPIED.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	HUMIDITY("HUMIDITY", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			// We need to change this later as I'm not sure how the humidity
			// reported.
			return new PointValue(PointType.Numeric, t.getIntValue());
		}
	},

	// Others attributes for switch and phtosensors
	SWITCH("SWITCH", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	PHOTO_SENSOR_PERCENT("PHOTO_SENSOR_PERCENT", Unit.PERCENTAGE, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},
	PHOTO_SENSOR_LUX("PHOTO_SENSOR_LUX", Unit.LUX, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	// Temperature Sensors
	TEMPERATURE_CURRENT("TEMPERATURE_CURRENT", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},

	TEMPERATURE_THERMOSTAT("TEMPERATURE_THERMOSTAT", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_OUTSIDE("TEMPERATURE_OUTSIDE", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_COOLER("TEMPERATURE_COOLER", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_FREEZER("TEMPERATURE_FREEZER", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_DUCT_MIXED_AIR("TEMPERATURE_DUCT_MIXED_AIR", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_DUCT_SUPPLY_AIR("TEMPERATURE_DUCT_SUPPLY_AIR", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_DUCT_RETURN_AIR("TEMPERATURE_DUCT_RETURN_AIR", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	TEMPERATURE_SPACE_REMOTE("TEMPERATURE_SPACE_REMOTE", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},

	// Thermostat related
	TEMPERATURE_SETPOINT_COOLING("TEMPERATURE_SETPOINT_COOLING", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},
	HVAC_COOLING_STPT_OVR("HVAC_COOLING_STPT_OVR", Unit.NONE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	TEMPERATURE_SETPOINT_HEATING("TEMPERATURE_SETPOINT_HEATING", Unit.DEGREE_MILLI_CELSIUS, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return temperatureValue(t.getFloatValue());
		}
	},

	HVAC_HEATING_STPT_OVR("HVAC_HEATING_STPT_OVR", Unit.NONE, Value.TEXT) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	THERMOSTAT_SYSTEM_MODE("THERMOSTAT_SYSTEM_MODE", Unit.SYSTEM_MODE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Enum, t.getTextValue());
		}
	},

	// This is map to rtuStage which support to report the stage number in int.
	//https://devcloud.swcoe.ge.com/devspace/display/AOUUL/Site+Pages
	THERMOSTAT_RUNNING_STATE("THERMOSTAT_RUNNING_STATE", Unit.RUNNING_STATE, Value.TEXT) {
		public PointValue apply(EventEntry t) {
			final String[] values = t.getTextValue().split(",");
			if (values != null && values.length > 0) {
				// Find Heating, Cooling Number
				int stage = 0;
				for (String value : values) {
					if (value.contains("COOLING_") || value.contains("HEATING_")) {
						try {
							int valueI = Integer.parseInt(value.replace("COOLING_", "").replace("HEATING_","").trim());
						    stage = stage < valueI ? valueI : stage;
						} catch (Exception ex) {
							// ignore
						}
					}
				}
				return new PointValue(PointType.Numeric, stage);
			} else {
				return new PointValue(PointType.Numeric, 0);
			}
		}
	},

	THERMOSTAT_MANUAL_OVERRIDE("THERMOSTAT_MANUAL_OVERRIDE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean, t.getIntValue() == 1 ? Boolean.TRUE : Boolean.FALSE);
		}
	},

	// Thermostat state detail
	THERMOSTAT_OFF_STATE("THERMOSTAT_OFF_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_HEATING_1_STATE("THERMOSTAT_HEATING_1_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_HEATING_2_STATE("THERMOSTAT_HEATING_2_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_HEATING_3_STATE("THERMOSTAT_HEATING_3_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_COOLING_1_STATE("THERMOSTAT_COOLING_1_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_COOLING_2_STATE("THERMOSTAT_COOLING_2_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_FAN_1_STATE("THERMOSTAT_FAN_1_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_FAN_2_STATE("THERMOSTAT_FAN_2_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_FAN_3_STATE("THERMOSTAT_FAN_3_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_EMERGENCY_HEATING_STATE("THERMOSTAT_EMERGENCY_HEATING_STATE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	// Thermostat running mode
	THERMOSTAT_OFF_MODE("THERMOSTAT_OFF_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_AUTO_MODE("THERMOSTAT_AUTO_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_COOLING_MODE("THERMOSTAT_COOLING_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_HEATING_MODE("THERMOSTAT_HEATING_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_EMERGENCY_HEATING_MODE("THERMOSTAT_EMERGENCY_HEATING_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	THERMOSTAT_OFF_MONITOR_MODE("THERMOSTAT_OFF_MONITOR_MODE", Unit.ON_OFF, Value.BIGINT) {
		public PointValue apply(EventEntry t) {
			return thermostatStageValue(t.getIntValue());
		}
	},

	// Others
	SYSTEM_LOAD("SYSTEM_LOAD", Unit.NONE, Value.TEXT) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	// Energy related
	ENERGY_GENERATED("ENERGY_GENERATED", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE("ENERGY_USAGE", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_KW("ENERGY_USAGE_KW", Unit.kW, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_LIGHT("ENERGY_USAGE_LIGHT", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_HEAT_GAS("ENERGY_USAGE_HEAT_GAS", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_HEAT_ELEC("ENERGY_USAGE_HEAT_ELEC", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_COOL_ELEC("ENERGY_USAGE_COOL_ELEC", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	ENERGY_USAGE_FAN_ELEC("ENERGY_USAGE_FAN_ELEC", Unit.kWh, Value.DOUBLE) {
		public PointValue apply(EventEntry t) {
			return energyValue(t.getFloatValue());
		}
	},

	// 0 - 10V devices
	HUMIDITY_SENSOR("HUMIDITY_SENSOR", Unit.PERCENTAGE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CO2_SENSOR("CO2_SENSOR", Unit.PARTS_PER_MILLION, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CO_SENSOR("CO_SENSOR", Unit.PARTS_PER_MILLION, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	VOC_SENSOR("VOC_SENSOR", Unit.PARTS_PER_MILLION, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_FAN("CRNT_FAN", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_CONDENSER_FAN("CRNT_CONDENSER_FAN", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_COMPRESSOR("CRNT_COMPRESSOR", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_COMPRESSOR_HPUMP("CRNT_COMPRESSOR_HPUMP", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_CONDENSER("CRNT_CONDENSER", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_CONDENSER_HPUMP("CRNT_CONDENSER_HPUMP", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_FURNACE("CRNT_FURNACE", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	CRNT_AUXILIARY_HEAT("CRNT_AUXILIARY_HEAT", Unit.AMPERE, Value.DOUBLE) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Numeric, t.getFloatValue());
		}
	},

	WATER_LEAK_DETECTOR("WATER_LEAK_DETECTOR", Unit.NONE, Value.TEXT) {
		@Override
		public PointValue apply(EventEntry t) {
			return new PointValue(PointType.Boolean,
					OverrideState.ON.name().equals(t.getTextValue()) ? Boolean.TRUE : Boolean.FALSE);
		}
	};

	private String name;
	private Unit unit;
	private Value valueType;

	Attribute(final String name, final Unit unit, final Value valueType) {
		this.name = name;
		this.unit = unit;
		this.valueType = valueType;
	}

	public String getName() {
		return this.name;
	}

	/**
	 * Get the given attribute by its name.
	 * 
	 * @param name
	 *            the the name of the attribute
	 * @return the attribute
	 */
	public static Attribute fromName(final String name) {
		Attribute result = UNKNOWN;
		for (final Attribute t : Attribute.values()) {
			if (t.name.equals(name)) {
				result = t;
				break;
			}
		}
		return result;
	}

	@Override
	public String toString() {
		return this.name();
	}

	public Unit getUnit() {
		return unit;
	}

	public Value getValueType() {
		return valueType;
	}

	private static PointValue temperatureValue(final float c) {
		return new PointValue(PointType.Numeric, Float.valueOf(32 + (c * (9 / 5.0f))));
	}

	private static PointValue thermostatStageValue(final int i) {
		return new PointValue(PointType.Enum, i == 1 ? "ON" : "OFF");
	}

	private static PointValue energyValue(final float f) {
		return new PointValue(PointType.Numeric, f);
	}
}
