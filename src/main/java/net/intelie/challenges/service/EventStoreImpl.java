package net.intelie.challenges.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;
import net.intelie.challenges.util.Utils;

public class EventStoreImpl implements EventStore {

  private final Map<String, ConcurrentSkipListMap<Long, Event>> events = new ConcurrentHashMap<>();

  @Override
  public void insert(Event event) {
    checkEvent(event);
    events.putIfAbsent(event.type(), new ConcurrentSkipListMap<>());
    events.get(event.type()).put(event.timestamp(), event);
  }

  @Override
  public void removeAll(String type) {
    checkEventType(type);
    events.remove(type);
  }

  @Override
  public EventIterator query(String type, long startTime, long endTime) {
    checkEventType(type);
    checkQueryInterval(startTime, endTime);

    ConcurrentNavigableMap<Long, Event> selectedEventsView = events.getOrDefault(type, new ConcurrentSkipListMap<>())
      .subMap(startTime, endTime);

    return new EventIteratorImpl(selectedEventsView);
  }

  private void checkQueryInterval(long startTime, long endTime) {
    if (startTime > endTime) {
      throw new IllegalArgumentException("Start time is bigger than end time.");
    }
  }

  private void checkEvent(Event event) {
    Utils.requireNonNull(event, "Event cannot be null.");
    checkEventType(event.type());
  }

  private void checkEventType(String type) {
    Utils.requireNonNull(type, "Event type cannot be null.");
    Utils.requireNonNull(EventType.getByName(type), String.format("Event type '%s' not supported.", type));
  }

}