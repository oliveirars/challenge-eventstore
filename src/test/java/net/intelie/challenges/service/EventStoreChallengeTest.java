package net.intelie.challenges.service;

import org.junit.After;

/**
 * Base test class. It aims to incorporate shared methods and attributes that
 * are used by the test classes.
 */
public class EventStoreChallengeTest {

  /** The event iterator. */
  protected EventIterator eventIterator;

  /** Close the iterator after each test method. */
  @After
  public void releaseResources() {
    if (eventIterator != null) {
      try {
        eventIterator.close();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      finally {
        eventIterator = null;
      }
    }
  }

}
