package net.intelie.challenges.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

/** Unit tests for {@link EventStoreImpl}. */
public class EventStoreTest extends EventStoreChallengeTest {

  /** The event store to be tested. */
  private EventStore eventStore;

  /** Creates a new event store to each test method. */
  @Before
  public void setup() {
    this.eventStore = new EventStoreImpl();
  }

  /**
   * Tests if <code>insert</code> throws {@link IllegalArgumentException} when
   * event to be inserted is <code>null</code>.
   */
  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventIsNull() {
    eventStore.insert(null);
  }

  /**
   * Tests if <code>insert</code> throws {@link IllegalArgumentException} when
   * event type is <code>null</code>.
   */
  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    eventStore.insert(new Event(null, 0));
  }

  /**
   * Tests if <code>insert</code> throws {@link IllegalArgumentException} when
   * event type is not supported.
   */
  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    eventStore.insert(new Event("MyType", 0));
  }

  /**
   * Tests if <code>insert</code> adds a new event when its type is not present
   * yet in the store.
   */
  @Test
  public void insert_ShouldInsertItem_When_EventTypeIsAbsent() {
    Event event = EventDataRepository.getEventsDataSet().get(0);

    eventStore.insert(event);
    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertTrue(eventIterator.moveNext());
    assertEquals(event, eventIterator.current());
  }

  /**
   * Tests if <code>insert</code> adds a new event when its type is already
   * present in the store.
   */
  @Test
  public void insert_ShouldInsertItem_When_EventTypeIsPresent() {
    Event event_1 = EventDataRepository.getEventsDataSet().get(0);
    Event event_2 = EventDataRepository.getEventsDataSet().get(1);

    eventStore.insert(event_1);
    eventStore.insert(event_2);
    eventIterator = eventStore.query(event_2.type(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertTrue(eventIterator.moveNext());
    assertEquals(event_1, eventIterator.current());
    assertTrue(eventIterator.moveNext());
    assertEquals(event_2, eventIterator.current());
  }

  /**
   * Tests if <code>removeAll</code> removes all events of the required type.
   */
  @Test
  public void removeAll_ShouldClearEventsByType() {
    populateStore(Optional.empty());
    String type = EventType.TYPE_1.toString();

    eventStore.removeAll(type);
    eventIterator = eventStore.query(type, Long.MIN_VALUE, Long.MAX_VALUE);

    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>removeAll</code> removes only events of the required type.
   */
  @Test
  public void removeAll_ShouldKeepEventsWithOtherTypes() {
    populateStore(Optional.empty());

    eventStore.removeAll(EventType.TYPE_1.toString());
    eventIterator = eventStore.query(EventType.TYPE_2.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertTrue(eventIterator.moveNext());
  }

  /**
   * Tests if <code>removeAll</code> throws {@link IllegalArgumentException} if
   * event type is null.
   */
  @Test(expected = IllegalArgumentException.class)
  public void removeAll_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    populateStore(Optional.empty());
    eventStore.removeAll(null);
  }

  /**
   * Tests if <code>removeAll</code> throws {@link IllegalArgumentException} if
   * event type is not supported.
   */
  @Test(expected = IllegalArgumentException.class)
  public void removeAll_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    populateStore(Optional.empty());
    eventStore.removeAll("MyType");
  }

  /**
   * Tests if <code>query</code> throws {@link IllegalArgumentException} if
   * event type is null.
   */
  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query(null, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   * Tests if <code>query</code> throws {@link IllegalArgumentException} if
   * event type is not supported.
   */
  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query("MyType", Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   * Tests if <code>query</code> throws {@link IllegalArgumentException} if
   * start time is bigger than end time.
   */
  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_StartTimeIsBiggerThanEndTime() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), 2, 1);
  }

  /**
   * Tests if <code>query</code> returns empty iterator when store has no events
   * of the required type.
   */
  @Test
  public void query_ShouldReturnEmptyIterator_When_ThereIsNoEventsOfType() {
    populateStore(Optional.of(Collections.singleton(EventType.TYPE_1)));
    eventIterator = eventStore.query(EventType.TYPE_2.toString(), Long.MIN_VALUE, Long.MAX_VALUE);
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>query</code> returns empty iterator when start time is
   * equals to end time.
   */
  @Test
  public void query_ShouldReturnEmptyIterator_When_StartTimeEqualsEndTime() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);
    eventIterator = eventStore.query(event.type(), event.timestamp(), event.timestamp());
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>query</code> returns empty iterator when there is no event
   * within the time window.
   */
  @Test
  public void query_ShouldReturnEmptyIterator_When_ThereIsNoEventsInTimeWindow() {
    Event event = EventDataRepository.getEventsDataSet().get(0);

    eventIterator = eventStore.query(event.type(), event.timestamp() + 1, Long.MAX_VALUE);
    assertFalse(eventIterator.moveNext());

    eventStore.insert(event);
    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, event.timestamp());
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>query</code> returns an iterator with events when time
   * window is tight to the event timestamp.
   */
  @Test
  public void query_ShouldReturnEvent_When_TimeWindowIsTight() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);

    eventIterator = eventStore.query(event.type(), event.timestamp(), event.timestamp() + 1);
    assertTrue(eventIterator.moveNext());
    assertEquals(event, eventIterator.current());
  }

  /**
   * Tests if <code>query</code> returns an iterator with events when there are
   * events within the time window
   */
  @Test
  public void query_ShouldReturnEvents_When_TimestampWithinTimeWindow() {
    populateStore(Optional.empty());
    List<Event> typeOneEvents = EventDataRepository.getEventsDataSetByType(EventType.TYPE_1);
    long startTime = typeOneEvents.get(0).timestamp();
    long endTime = typeOneEvents.get(typeOneEvents.size() - 2).timestamp() + 1;

    eventIterator = eventStore.query(EventType.TYPE_1.toString(), startTime, endTime);

    for (int i = 0; i < typeOneEvents.size() - 1; ++i) {
      assertTrue(eventIterator.moveNext());
      assertEquals(typeOneEvents.get(i), eventIterator.current());
    }
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>query</code> returns an iterator without a removed event.
   */
  @Test
  public void query_ShouldReturnIteratorWithoutRemovedEvent_When_CalledAfterEventRemove() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);

    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, Long.MAX_VALUE);
    eventIterator.moveNext();
    eventIterator.remove();
    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertFalse(eventIterator.moveNext());
  }

  /**
   * Utility method used to populate the event store using the test dataset as
   * source.
   * 
   * @param types Types of events to populate the store. If not present, all
   *        events will be used, despite their types.
   */
  private void populateStore(Optional<Set<EventType>> types) {
    boolean allTypes = !types.isPresent();
    Set<String> requestedTypes = types.orElse(Collections.emptySet()).stream().map(type -> type.toString()).collect(
      Collectors.toSet());

    EventDataRepository.getEventsDataSet().stream().filter(event -> allTypes || requestedTypes.contains(event.type()))
      .forEach(event -> eventStore.insert(event));
  }

}