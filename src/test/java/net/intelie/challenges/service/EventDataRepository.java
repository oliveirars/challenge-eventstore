package net.intelie.challenges.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

/** Repository of events to be used on unit tests. */
public class EventDataRepository {

  /** Events dataset. */
  private static List<Event> TEST_DATASET;

  /**
   * Gets a collection of test events. These events are used on unit tests.
   * 
   * @return List of test events.
   */
  public static List<Event> getEventsDataSet() {

    if (TEST_DATASET == null) {
      Event event_t1_00 = new Event(EventType.TYPE_1.toString(), 10l);
      Event event_t1_01 = new Event(EventType.TYPE_1.toString(), 20l);
      Event event_t1_02 = new Event(EventType.TYPE_1.toString(), 30l);
      Event event_t1_03 = new Event(EventType.TYPE_1.toString(), 40l);

      Event event_t2_00 = new Event(EventType.TYPE_2.toString(), 10l);
      Event event_t2_01 = new Event(EventType.TYPE_2.toString(), 20l);
      Event event_t2_02 = new Event(EventType.TYPE_2.toString(), 30l);

      TEST_DATASET = new ArrayList<>(Arrays.asList(event_t1_00, event_t1_01, event_t1_02, event_t1_03, event_t2_00,
        event_t2_01, event_t2_02));
    }
    return TEST_DATASET;
  }

  /**
   * Gets a collection of test events filtered by type.
   * 
   * @param type The type of events.
   * @return A collection containning only events of the required type.
   */
  public static List<Event> getEventsDataSetByType(EventType type) {
    return getEventsDataSet().stream().filter(event -> type.toString().equals(event.type())).collect(Collectors
      .toList());
  }

}