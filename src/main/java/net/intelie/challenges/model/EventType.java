package net.intelie.challenges.model;

import java.util.Arrays;

public enum EventType {

  TYPE_1("TYPE_1"),
  TYPE_2("TYPE_2");

  private final String name;

  public static EventType getByName(String typeName) {
    return Arrays.asList(values()).stream().filter(type -> type.name.equals(typeName)).findFirst().orElse(null);
  }

  private EventType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

}