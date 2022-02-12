package net.intelie.challenges.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

public class EventDataRepository {

  private static List<Event> TEST_DATASET;

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

  public static List<Event> getEventsDataSetByType(EventType type) {
    return getEventsDataSet().stream().filter(event -> type.toString().equals(event.type())).collect(Collectors
      .toList());
  }

}