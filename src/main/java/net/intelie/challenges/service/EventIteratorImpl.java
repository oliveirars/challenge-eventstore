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

  /** The wrapped event iterator. This one iterates over the dataset keys. */
  private Iterator<Long> iterator;

  /** The current event key. It is the key pointed by the iterator. */
  private Long currentEventKey;

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
      iterator = this.events.keySet().iterator();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean moveNext() {
    /* Iterator is invalid, closed, or has no more events to visit. */
    if (iterator == null || !iterator.hasNext()) {
      currentEventKey = null;
      return false;
    }
    currentEventKey = iterator.next();
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Event current() {
    checkState();
    return events.get(currentEventKey);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove() {
    checkState();
    iterator.remove();
    currentEventKey = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws Exception {
    currentEventKey = null;
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
    if (currentEventKey == null) {
      throw new IllegalStateException("There is no current event in iteration.");
    }
  }

}