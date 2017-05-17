package com.ge.current.em.analytics.dto.daintree.attribute;

public enum Unit {
    NONE("NONE"),
    AMPERE("AMPERE"),
    DEGREE_CELSIUS("DEGREE_CELSIUS"),
    DEGREE_MILLI_CELSIUS("DEGREE_MILLI_CELSIUS"),
    SYSTEM_MODE("SYSTEM_MODE"),
    ON_OFF("ON_OFF"),
    PARTS_PER_MILLION("PARTS_PER_MILLION"),
    RUNNING_STATE("RUNNING_STATE"),
    PERCENTAGE("PERCENTAGE"),
    WATTS("WATTS"),
    kWh("kWh"),
    kW("kW"),
    LUX("LUX");

    private String name;

    Unit(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    /**
     * Get the unit from its name.
     * @param name the name of the unit
     * @return the unit
     */
    public static Unit fromName(final String name) {
      Unit result = NONE;
      for (final Unit t : Unit.values()) {
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
}
