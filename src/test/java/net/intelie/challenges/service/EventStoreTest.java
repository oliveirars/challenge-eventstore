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

public class EventStoreTest extends EventStoreChallengeTest {

  private EventStore eventStore;

  @Before
  public void setup() {
    this.eventStore = new EventStoreImpl();
  }

  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventIsNull() {
    eventStore.insert(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    eventStore.insert(new Event(null, 0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void insert_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    eventStore.insert(new Event("MyType", 0));
  }

  @Test
  public void insert_ShouldInsertItem_When_EventTypeIsAbsent() {
    Event event = EventDataRepository.getEventsDataSet().get(0);

    eventStore.insert(event);
    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertTrue(eventIterator.moveNext());
    assertEquals(event, eventIterator.current());
  }

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

  @Test
  public void removeAll_ShouldClearEventsByType() {
    populateStore(Optional.empty());
    String type = EventType.TYPE_1.toString();

    eventStore.removeAll(type);
    eventIterator = eventStore.query(type, Long.MIN_VALUE, Long.MAX_VALUE);

    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void removeAll_ShouldKeepEventsWithOtherTypes() {
    populateStore(Optional.empty());

    eventStore.removeAll(EventType.TYPE_1.toString());
    eventIterator = eventStore.query(EventType.TYPE_2.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertTrue(eventIterator.moveNext());
  }

  @Test(expected = IllegalArgumentException.class)
  public void removeAll_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    populateStore(Optional.empty());
    eventStore.removeAll(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void removeAll_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    populateStore(Optional.empty());
    eventStore.removeAll("MyType");
  }

  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_EventTypeIsNull() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query(null, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_EventTypeNotSupported() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query("MyType", Long.MIN_VALUE, Long.MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void query_ShouldThrowIllegalArgument_When_StartTimeIsBiggerThanEndTime() {
    populateStore(Optional.empty());
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), 2, 1);
  }

  @Test
  public void query_ShouldReturnEmptyIterator_When_ThereIsNoEventsOfType() {
    populateStore(Optional.of(Collections.singleton(EventType.TYPE_1)));
    eventIterator = eventStore.query(EventType.TYPE_2.toString(), Long.MIN_VALUE, Long.MAX_VALUE);
    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void query_ShouldReturnEmptyIterator_When_StartTimeEqualsEndTime() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);

    eventIterator = eventStore.query(event.type(), event.timestamp(), event.timestamp());

    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void query_ShouldReturnEmptyIterator_When_ThereIsNoEventsInTimeWindow() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);

    eventIterator = eventStore.query(event.type(), event.timestamp() + 1, Long.MAX_VALUE);
    assertFalse(eventIterator.moveNext());

    eventIterator = eventStore.query(event.type(), Long.MIN_VALUE, event.timestamp());
    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void query_ShouldReturnEvent_When_TimeWindowIsTight() {
    Event event = EventDataRepository.getEventsDataSet().get(0);
    eventStore.insert(event);

    eventIterator = eventStore.query(event.type(), event.timestamp(), event.timestamp() + 1);
    assertTrue(eventIterator.moveNext());
    assertEquals(event, eventIterator.current());
  }

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

  private void populateStore(Optional<Set<EventType>> types) {
    boolean allTypes = !types.isPresent();
    Set<String> requestedTypes = types.orElse(Collections.emptySet()).stream().map(type -> type.toString()).collect(
      Collectors.toSet());

    EventDataRepository.getEventsDataSet().stream().filter(event -> allTypes || requestedTypes.contains(event.type()))
      .forEach(event -> eventStore.insert(event));
  }

}