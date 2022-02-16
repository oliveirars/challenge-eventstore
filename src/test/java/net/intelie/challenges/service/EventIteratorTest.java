package net.intelie.challenges.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

/** Unit tests for {@link EventIteratorImpl}. */
public class EventIteratorTest extends EventStoreChallengeTest {

  /** Local copy of the test dataset. */
  private static List<Event> DATASET;

  /**
   * Creates a data view do initialize the test iterator.
   * 
   * @param events List of events to be included in the data view.
   * @return A {@link TreeMap} containing all events received as parameter.
   */
  private static Map<Long, Event> createDataView(List<Event> events) {
    Map<Long, Event> dataView = new TreeMap<>();
    events.forEach(event -> dataView.put(event.timestamp(), event));
    return dataView;
  }

  /** Loads the local dataset before all tests. */
  @BeforeClass
  public static void init() {
    DATASET = EventDataRepository.getEventsDataSet();
  }

  /**
   * Tests if <code>moveNext</code> returns <code>true</code> when iterator has
   * a next event.
   */
  @Test
  public void moveNext_ShouldReturnTrue_When_IterationHasMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    assertTrue(eventIterator.moveNext());
  }

  /**
   * Tests if <code>moveNext</code> returns <code>false</code> when iterator was
   * initialized with an empty dataset.
   */
  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasNoEvents() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>moveNext</code> returns <code>false</code> when iterator was
   * initialized with <code>null</code>.
   */
  @Test
  public void moveNext_ShouldReturnFalse_When_IterationInitializedWithNull() {
    eventIterator = new EventIteratorImpl(null);
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>moveNext</code> returns <code>false</code> when it reaches
   * the end.
   */
  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasNoMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    assertTrue(eventIterator.moveNext());
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>moveNext</code> returns <code>false</code> when iterator is
   * closed.
   */
  @Test
  public void moveNext_ShouldReturnFalse_When_IterationHasBeenClosed() throws Exception {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 2)));
    eventIterator.moveNext();
    eventIterator.close();
    assertFalse(eventIterator.moveNext());
  }

  /**
   * Tests if <code>current</code> throws {@link IllegalStateException} when
   * called without a previous call on <code>moveNext</code>.
   */
  @Test(expected = IllegalStateException.class)
  public void current_ShouldThrowIllegalState_When_CalledWithoutPreviousMoveNext() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET));
    eventIterator.current();
  }

  /**
   * Tests if <code>current</code> throws {@link IllegalStateException} when
   * iterator has no current event.
   */
  @Test(expected = IllegalStateException.class)
  public void current_ShouldThrowIllegalState_When_IterationHasNoEvents() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    eventIterator.moveNext();
    eventIterator.current();
  }

  /**
   * Tests if <code>current</code> throws {@link IllegalStateException} when
   * iterator reached the end.
   */
  @Test(expected = IllegalStateException.class)
  public void current_ShouldThrowIllegalState_When_IterationHasNoMoreEvents() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.moveNext();
    eventIterator.moveNext();
    eventIterator.current();
  }

  /**
   * Tests if <code>current</code> throws {@link IllegalStateException} when
   * iterator is closed.
   */
  @Test(expected = IllegalStateException.class)
  public void current_ShouldThrowIllegalState_When_IterationHasBeenClosed() throws Exception {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 3)));
    eventIterator.moveNext();
    eventIterator.close();
    eventIterator.current();
  }

  /**
   * Tests if <code>current</code> returns correct events according with the
   * input dataset.
   */
  @Test
  public void current_ShouldReturnEvent_When_IterationHasCurrentEvent() {
    List<Event> type1Dataset = EventDataRepository.getEventsDataSetByType(EventType.TYPE_1);
    eventIterator = new EventIteratorImpl(createDataView(type1Dataset));

    for (Event event : type1Dataset) {
      eventIterator.moveNext();
      assertEquals(event, eventIterator.current());
    }
  }

  /**
   * Tests if <code>remove</code> throws {@link IllegalStateException} when
   * iterator was initialized with empty dataset.
   */
  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationHasNoEvent() {
    eventIterator = new EventIteratorImpl(createDataView(Collections.emptyList()));
    eventIterator.moveNext();
    eventIterator.remove();
  }

  /**
   * Tests if <code>remove</code> throws {@link IllegalStateException} when
   * iterator was initialized with <code>null</code>.
   */
  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationInitializedWithNul() {
    eventIterator = new EventIteratorImpl(null);
    eventIterator.moveNext();
    eventIterator.remove();
  }

  /**
   * Tests if <code>remove</code> throws {@link IllegalStateException} when
   * iterator does not have a current event.
   */
  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_IterationHasNoMoreEvent() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.moveNext();
    eventIterator.moveNext();
    eventIterator.remove();
  }

  /**
   * Tests if <code>remove</code> throws {@link IllegalStateException} when it
   * is called without previous call on <code>moveNext</code>.
   */
  @Test(expected = IllegalStateException.class)
  public void remove_ShouldThrowIllegalState_When_CalledWithoutPreviousMoveNext() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 1)));
    eventIterator.remove();
  }

  /** Tests if <code>remove</code> does not automatically move to next event. */
  @Test(expected = IllegalStateException.class)
  public void remove_ShouldNotMoveNext_When_CalledOverEvent() {
    eventIterator = new EventIteratorImpl(createDataView(DATASET.subList(0, 2)));
    eventIterator.moveNext();
    eventIterator.remove();
    eventIterator.current();
  }

  /**
   * Tests if <code>remove</code> deletes the current event from the underlying
   * dataset.
   */
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

}