package net.intelie.challenges.service;

import java.util.Iterator;
import java.util.Map;

import net.intelie.challenges.model.Event;

public class EventIteratorImpl implements EventIterator {

  private final Map<Long, Event> events;
  private Iterator<Event> iterator;
  private Event currentEvent;

  public EventIteratorImpl(Map<Long, Event> events) {
    this.events = events;
    if (events != null) {
      iterator = this.events.values().iterator();
    }
  }

  @Override
  public boolean moveNext() {
    if (iterator == null || !iterator.hasNext()) {
      currentEvent = null;
      return false;
    }
    currentEvent = iterator.next();
    return true;
  }

  @Override
  public Event current() {
    if (iterator == null) {
      throw new IllegalStateException("The iteration is closed.");
    }
    checkCurrentEvent();
    return currentEvent;
  }

  @Override
  public void remove() {
    checkCurrentEvent();
    iterator.remove();
    currentEvent = null;
  }

  @Override
  public void close() throws Exception {
    currentEvent = null;
    iterator = null;
  }

  private void checkCurrentEvent() {
    if (currentEvent == null) {
      throw new IllegalStateException("There is no current event in iteration.");
    }
  }

}