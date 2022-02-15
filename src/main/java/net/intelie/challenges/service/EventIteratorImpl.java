package net.intelie.challenges.service;

import java.util.Iterator;
import java.util.Map;

import net.intelie.challenges.model.Event;

/**
 * Implements an iterator for a map of {@link Event}. This class works as a
 * wrapper, providing validations and methods to iterate over the underlying
 * event dataset.
 */
public class EventIteratorImpl implements EventIterator {

  /** Event dataset. In this map, each event is mapped by its timestamp. */
  private final Map<Long, Event> events;

  /** The wrapped event iterator. */
  private Iterator<Event> iterator;

  /** The current event. It is the event pointed by the iterator. */
  private Event currentEvent;

  /**
   * Constructor. Creates a new instance of {@link EventIteratorImpl}.
   *
   * @param events The underlying dataset to be visited by the iterator. If this
   *        dataset is <code>null</code>, the iterator will also be null,
   *        indicating it is closed.
   */
  public EventIteratorImpl(Map<Long, Event> events) {
    this.events = events;
    if (events != null) {
      iterator = this.events.values().iterator();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean moveNext() {
    /* Iterator is invalid, closed, or has no more events to visit. */
    if (iterator == null || !iterator.hasNext()) {
      currentEvent = null;
      return false;
    }
    currentEvent = iterator.next();
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event current() {
    checkState();
    return currentEvent;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove() {
    checkState();
    iterator.remove();
    currentEvent = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws Exception {
    currentEvent = null;
    iterator = null;
  }

  /**
   * Checks the iteration state. The state is considered illegal if either the
   * inner iterator or the current event is <code>null</code>.
   */
  private void checkState() {
    if (iterator == null) {
      throw new IllegalStateException("The iteration is closed.");
    }
    if (currentEvent == null) {
      throw new IllegalStateException("There is no current event in iteration.");
    }
  }

}