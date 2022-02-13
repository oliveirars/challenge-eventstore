package net.intelie.challenges.service;

import org.junit.After;

public class EventStoreChallengeTest {

  protected EventIterator eventIterator;

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
