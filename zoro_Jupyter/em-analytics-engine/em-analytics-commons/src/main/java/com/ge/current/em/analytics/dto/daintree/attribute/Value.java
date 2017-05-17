package com.ge.current.em.analytics.dto.daintree.attribute;

public enum Value {
    NONE("NONE"),
    BIGINT("BIGINT"),
    DOUBLE("DOUBLE"),
    TEXT("TEXT");

    private String name;

    Value(final String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    /**
     * Get the value from its name.
     *
     * @param name the name
     * @return the value
     */
    public static Value fromName(final String name) {
      Value result = NONE;
      for (final Value t : Value.values()) {
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
