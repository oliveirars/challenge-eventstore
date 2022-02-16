package net.intelie.challenges.model;

import java.util.Arrays;

/** Utility enumeration used to define the supported event types. */
public enum EventType {

  /** Type 1 */
  TYPE_1("TYPE_1"),

  /** Type 2 */
  TYPE_2("TYPE_2");

  /** Type name. */
  private final String name;

  /**
   * Gets an event type according to its name.
   *
   * @param typeName The name of the type to be returned.
   * @return An event type with the name requested in typeName.
   */
  public static EventType getByName(String typeName) {
    return Arrays.asList(values()).stream().filter(type -> type.name.equals(typeName)).findFirst().orElse(null);
  }

  /**
   * Constructor. Creates a new instance of {@link EventType}.
   *
   * @param name The type name.
   */
  private EventType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

}