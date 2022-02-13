package net.intelie.challenges.service;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

public class EventStoreImpl implements EventStore {

  private Map<String, List<Event>> map;
  private final Map<String, List<Event>> events = new ConcurrentHashMap<>();

  @Override
  public void insert(Event event) {
    checkEvent(event);
    events.putIfAbsent(event.type(), new LinkedList<>());
    events.get(event.type()).add(event);
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

    Predicate<Event> filterCriteria = event -> startTime <= event.timestamp() && event.timestamp() < endTime;

    List<Event> selectedEventsView = events.getOrDefault(type, new LinkedList<>()).stream().filter(filterCriteria)
      .collect(Collectors.toList());

    return new EventIteratorImpl(selectedEventsView);
  }

  private void checkQueryInterval(long startTime, long endTime) {
    if (startTime > endTime) {
      throw new IllegalArgumentException("Start time is bigger than end time.");
    }
  }

  private void checkEvent(Event event) {
    if (event == null) {
      throw new IllegalArgumentException("Event cannot be null.");
    }
    checkEventType(event.type());
  }

  private void checkEventType(String type) {
    if (type == null) {
      throw new IllegalArgumentException("Event type cannot be null.");
    }
    if (EventType.getByName(type) == null) {
      throw new IllegalArgumentException(String.format("Event type '%s' not supported.", type));
    }
  }

}
