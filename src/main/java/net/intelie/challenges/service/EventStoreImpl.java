package net.intelie.challenges.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;
import net.intelie.challenges.util.Utils;

/**
 * Implements a concurrent {@link EventStore}. This class implements the
 * interface methods allowing concurrent access to its operations.
 */
public class EventStoreImpl implements EventStore {

  /**
   * Data structure to store all events. This map organizes the events according
   * to their types. All events of a type (the key of the map) are stored in a
   * inner map that uses their timestamp as keys and the events themselves as
   * values.
   * 
   * The {@link ConcurrentHashMap} provides fast and thread-safe operations,
   * which is crucial to support concurrent accesses.
   * 
   * The {@link ConcurrentSkipListMap}, which is used to store the events of a
   * type, is also time-efficient and thread-safe. Moreover, this data structure
   * keeps the entries sorted according to their keys, making easier the task of
   * filter by timestamp window.
   */
  private final Map<String, ConcurrentSkipListMap<Long, Event>> events = new ConcurrentHashMap<>();

  /**
   * Checks the validity of query time interval.
   * 
   * @param startTime Initial time (inclusive).
   * @param endTime Final time (exclusive).
   * @throws IllegalArgumentException if the initial time is bigger than final
   *         time.
   */
  private static void checkQueryInterval(long startTime, long endTime) {
    if (startTime > endTime) {
      throw new IllegalArgumentException("Start time is bigger than end time.");
    }
  }

  /**
   * Checks if an event is valid.
   * 
   * @param event The event to be checked.
   * @throws IllegalArgumentException if the event is null or if it has invalid
   *         type.
   */
  private static void checkEvent(Event event) {
    Utils.requireNonNull(event, "Event cannot be null.");
    checkEventType(event.type());
  }

  /**
   * Checks if an event type is valid.
   * 
   * @param eventType The type to be checked.
   * @throws IllegalArgumentException if the type is null or it is not
   *         supported.
   */
  private static void checkEventType(String eventType) {
    Utils.requireNonNull(eventType, "Event type cannot be null.");
    Utils.requireNonNull(EventType.getByName(eventType), String.format("Event type '%s' not supported.", eventType));
  }

  /**
   * {@inheritDoc}. If the event type is not present in the events map, a new
   * {@link ConcurrentSkipListMap} is added to store events of this type. At the
   * end, the event is just stored in the correct type map.
   * 
   * The cost of this operation is the cost of insert an element in the
   * {@link ConcurrentSkipListMap}, which is, in the worst case, O(n). In the
   * average case, the cost is O(log n), where n is the amount of stored events.
   */
  @Override
  public void insert(Event event) {
    checkEvent(event);
    events.putIfAbsent(event.type(), new ConcurrentSkipListMap<>());
    events.get(event.type()).put(event.timestamp(), event);
  }

  /**
   * {@inheritDoc} This operation is done in constant time because it just
   * removes an entry from the <type,events> map.
   */
  @Override
  public void removeAll(String type) {
    checkEventType(type);
    events.remove(type);
  }

  /**
   * {@inheritDoc} This method performs the query over the events store and
   * creates a data view to be used as underlying dataset of the returned
   * iterator. An important observation is that the view created by
   * <code>submap</code> method is linked with the original data structure. In
   * other words, changes made by the iterator will be reflected in the original
   * map, which is a required behavior.
   * 
   * The first step of this operation is to select the correct events according
   * with the required type. It is done using the retrieve operation on map,
   * which has constant time cost. After that, it call the <code>submap</code>
   * method which has time complexity equals to O(log n) in the average case. In
   * the worst case, it has O(n) complexity.
   */
  @Override
  public EventIterator query(String type, long startTime, long endTime) {
    checkEventType(type);
    checkQueryInterval(startTime, endTime);

    Map<Long, Event> selectedEventsView = events.getOrDefault(type, new ConcurrentSkipListMap<>()).subMap(startTime,
      endTime);

    return new EventIteratorImpl(selectedEventsView);
  }

}