package net.intelie.challenges.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

public class EventIteratorTest extends EventStoreChallengeTest {

  private static List<Event> DATASET;

  @BeforeClass
  public static void init() {
    DATASET = EventDataRepository.getEventsDataSet();
  }

  @Test
  public void moveNext_ShouldReturnTrue_When_IterationHasMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    assertTrue(eventIterator.moveNext());
  }

  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasNoEvents() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void moveNext_ShouldReturnFalse_When_IterationInitializedWithNull() {
    eventIterator = new EventIteratorImpl(null);
    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasNoMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    assertTrue(eventIterator.moveNext());
    assertFalse(eventIterator.moveNext());
  }

  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasBeenClosed() throws Exception {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 2)));
    eventIterator.moveNext();
    eventIterator.close();
    assertFalse(eventIterator.moveNext());
  }

  @Test(expected = NoSuchElementException.class)
  public void current_ShouldThrowNoSuchElement_When_CalledWithoutPreviousMoveNext() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET));
    eventIterator.current();
  }

  @Test(expected = NoSuchElementException.class)
  public void current_ShouldThrowNoSuchElement_When_IterationHasNoEvents() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    eventIterator.moveNext();
    eventIterator.current();
  }

  @Test(expected = NoSuchElementException.class)
  public void current_ShouldThrowNoSuchElement_When_IterationHasNoMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.moveNext();
    eventIterator.moveNext();
    eventIterator.current();
  }

  @Test(expected = IllegalStateException.class)
  public void current_ShouldThrowIllegalState_When_IterationHasBeenClosed() throws Exception {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.moveNext();
    eventIterator.close();
    eventIterator.current();
  }

  @Test
  public void current_ShouldReturnEvent_When_IterationHasCurrentEvent() {
    List<Event> type1Dataset = DATASET.stream().filter(event -> EventType.TYPE_1.toString().equals(event.type()))
      .collect(Collectors.toList());

    eventIterator = new EventIteratorImpl(createDataView(type1Dataset));

    for (Event event : type1Dataset) {
      eventIterator.moveNext();
      assertEquals(event, eventIterator.current());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationHasNoEvent() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    eventIterator.moveNext();
    eventIterator.remove();
  }

  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationInitializedWithNul() {
    eventIterator = new EventIteratorImpl(null);
    eventIterator.moveNext();
    eventIterator.remove();
  }

  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationHasNoMoreEvent() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.moveNext();
    eventIterator.moveNext();
    eventIterator.remove();
  }

  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_CalledWithoutPreviousMoveNext() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.remove();
  }

  @Test(expected = NoSuchElementException.class)
  public void remove_ShouldNotMoveNext_When_CalledOverEvent() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 2)));
    eventIterator.moveNext();
    eventIterator.remove();
    eventIterator.current();
  }

  public void remove_ShouldDeleteEventFromDataView_When_CalledOverEvent() {
    Event eventToRemove = DATASET.get(0);
    Event eventToKeep = DATASET.get(1);

    Map<Long, Event> dataView = createDataView(Arrays.asList(eventToKeep, eventToRemove));
    eventIterator = new EventIteratorImpl(dataView);

    eventIterator.moveNext();
    eventIterator.remove();

    assertEquals(1, dataView.size());
    assertFalse(dataView.containsKey(eventToRemove.timestamp()));
    assertEquals(dataView.get(eventToKeep.timestamp()), eventToKeep);
  }

  private Map<Long, Event> createDataView(List<Event> events) {
    Map<Long, Event> dataView = new TreeMap<>();
    events.forEach(event -> dataView.put(event.timestamp(), event));
    return dataView;
  }

}