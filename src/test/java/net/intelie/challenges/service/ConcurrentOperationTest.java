package net.intelie.challenges.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import org.junit.Before;
import org.junit.Test;

import net.intelie.challenges.model.Event;
import net.intelie.challenges.model.EventType;

public class ConcurrentOperationTest extends EventStoreChallengeTest {

  /** The event store to be tested. */
  private EventStore eventStore;

  private static void runAndWaitForThreads(List<Thread> threads) throws InterruptedException {
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private void generateEventsAndInsert(EventType type, long start, long end) {
    LongStream.range(start, end).boxed().map(timestamp -> new Event(type.toString(), timestamp)).forEach(
      eventStore::insert);
  }

  private List<Thread> generateInserters(EventType eventType, int numThreads, int eventsPerThread, int timeOffset) {
    List<Thread> threads = new ArrayList<>(numThreads);

    for (int i = 0; i < numThreads; ++i) {
      final int start = timeOffset + (i * eventsPerThread);
      final int end = start + eventsPerThread;

      threads.add(new Thread(() -> {
        generateEventsAndInsert(eventType, start, end);
      }));
    }
    return threads;
  }

  private List<Thread> generateRemovers(EventType eventType, int numThreads, int eventsPerThread) {
    List<Thread> threads = new ArrayList<>(numThreads);

    for (int i = 0; i < numThreads; ++i) {
      final int start = i * eventsPerThread;
      final int end = start + eventsPerThread;

      threads.add(new Thread(() -> {
        try (EventIterator iterator = eventStore.query(eventType.toString(), start, end)) {
          while (iterator.moveNext()) {
            iterator.remove();
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          fail(e.getMessage());
        }
      }));

    }
    return threads;
  }

  /** Creates a new event store to each test method. */
  @Before
  public void setup() {
    this.eventStore = new EventStoreImpl();
  }

  @Test
  public void insert_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    int numThreads = 10;
    int eventsPerThread = 5000;
    List<Thread> insertWorkers = generateInserters(EventType.TYPE_1, numThreads, eventsPerThread, 0);
    Collections.shuffle(insertWorkers);

    runAndWaitForThreads(insertWorkers);
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    int eventsCount = 0;
    while (eventIterator.moveNext()) {
      ++eventsCount;
    }

    assertEquals(numThreads * eventsPerThread, eventsCount);
  }

  @Test
  public void remove_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    int totalEvents = 50000;
    generateEventsAndInsert(EventType.TYPE_1, 0, totalEvents);

    int numThreads = 10;
    int eventsPerThread = totalEvents / numThreads;
    List<Thread> removeWorkers = generateRemovers(EventType.TYPE_1, numThreads, eventsPerThread);
    Collections.shuffle(removeWorkers);

    runAndWaitForThreads(removeWorkers);
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertFalse(eventIterator.moveNext());

  }

  @Test
  public void insertAndRemove_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    List<Thread> workers = new ArrayList<>();

    int previousStoredCount = 50000;
    generateEventsAndInsert(EventType.TYPE_1, 0, previousStoredCount);

    int numRemovers = 10;
    int eventsPerRemover = previousStoredCount / numRemovers;
    workers.addAll(generateRemovers(EventType.TYPE_1, numRemovers, eventsPerRemover));

    int numInserters = 10;
    int eventsPerInserter = 5000;
    workers.addAll(generateInserters(EventType.TYPE_1, numInserters, eventsPerInserter, previousStoredCount));

    Collections.shuffle(workers);

    runAndWaitForThreads(workers);
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    int eventsCount = 0;
    while (eventIterator.moveNext()) {
      ++eventsCount;
    }

    assertEquals(numInserters * eventsPerInserter, eventsCount);
  }

  @Test
  public void queryAndRemoveAll_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    List<Thread> workers = new ArrayList<>();
    AtomicInteger eventsCount = new AtomicInteger(0);

    int previousStoredCount = 50000;
    generateEventsAndInsert(EventType.TYPE_1, 0, previousStoredCount);

    workers.add(new Thread(() -> {
      eventStore.removeAll(EventType.TYPE_1.toString());
    }));

    workers.add(new Thread(() -> {
      try (EventIterator iterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE)) {
        while (iterator.moveNext()) {
          eventsCount.incrementAndGet();
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }));

    runAndWaitForThreads(workers);

    assertTrue(0 == eventsCount.intValue() || previousStoredCount == eventsCount.intValue());
  }

}
