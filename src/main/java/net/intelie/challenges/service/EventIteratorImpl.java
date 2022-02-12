package net.intelie.challenges.service;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import net.intelie.challenges.model.Event;

public class EventIteratorImpl implements EventIterator {

  private final List<Event> events;
  private Iterator<Event> iterator;
  private Event currentEvent;

  public EventIteratorImpl(List<Event> events) {
    this.events = events;
    if (events != null) {
      iterator = this.events.iterator();
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
    if (currentEvent == null) {
      throw new NoSuchElementException("There is no current event in iteration.");
    }
    return currentEvent;
  }

  @Override
  public void remove() {
    if (currentEvent == null) {
      throw new IllegalStateException("There is no current event in iteration.");
    }
    iterator.remove();
    currentEvent = null;
  }

  @Override
  public void close() throws Exception {
    currentEvent = null;
    iterator = null;
  }

}