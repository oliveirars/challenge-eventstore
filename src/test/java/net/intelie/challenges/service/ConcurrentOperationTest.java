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

/**
 * Tests for concurrent use of the event store operations.
 */
public class ConcurrentOperationTest extends EventStoreChallengeTest {

  /** The event store to be tested. */
  private EventStore eventStore;

  /**
   * Utility method used to execute a collection of threads and wait for their
   * conclusions. The method shuffles the collection before start, avoiding any
   * kind of order bias.
   * 
   * @param threads Collection of threads to be executed.
   * @throws InterruptedException
   */
  private static void runAndWaitForThreads(List<Thread> threads) throws InterruptedException {
    Collections.shuffle(threads);
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  /**
   * Generates a collection of events and store them in the event store.
   * 
   * @param type Type of the events.
   * @param start Initial timestamp.
   * @param end Final timestamp.
   */
  private void generateEventsAndInsert(EventType type, long start, long end) {
    LongStream.range(start, end).boxed().map(timestamp -> new Event(type.toString(), timestamp)).forEach(
      eventStore::insert);
  }

  /**
   * Generates a collection of threads responsible for inserting events in the
   * event store.
   * 
   * @param eventType Type of events.
   * @param numThreads Number of threads.
   * @param eventsPerThread Number of events to be inserted by each thread.
   * @param timeOffset An offset of timestamp used to define the range of events
   *        to be created.
   * @return The list of created threads.
   */
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

  /**
   * Generates a collection of threads responsible for deleting events from the
   * event store.
   * 
   * @param eventType Type of events.
   * @param numThreads Number of threads.
   * @param eventsPerThread Number of events to be deleted by each thread.
   * @return The list of created threads.
   */
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

  /**
   * Tests if concurrent insertions work without errors and if the result store
   * is correct.
   * 
   * This test creates a collection of inserter threads (each one has to insert
   * a lot of events) and spawns them. The test successfully ends if all the
   * events are inserted without concurrent access errors.
   * 
   * @throws InterruptedException
   */
  @Test
  public void insert_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    int numThreads = 10;
    int eventsPerThread = 5000;

    runAndWaitForThreads(generateInserters(EventType.TYPE_1, numThreads, eventsPerThread, 0));
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    int eventsCount = 0;
    while (eventIterator.moveNext()) {
      ++eventsCount;
    }

    assertEquals(numThreads * eventsPerThread, eventsCount);
  }

  /**
   * Tests if concurrent removals work without errors and if the result store is
   * correct.
   * 
   * This test populates the event store with thousands of events. After that,
   * it creates a collection of remover threads (each one has to remove the same
   * amount of events) and spawns them. The test successfully ends if all the
   * events are removed without concurrent access errors.
   * 
   * @throws InterruptedException
   */
  @Test
  public void remove_ShouldWork_Over_ConcurrentAcesses() throws InterruptedException {

    int totalEvents = 50000;
    generateEventsAndInsert(EventType.TYPE_1, 0, totalEvents);

    int numThreads = 10;
    int eventsPerThread = totalEvents / numThreads;

    runAndWaitForThreads(generateRemovers(EventType.TYPE_1, numThreads, eventsPerThread));
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    assertFalse(eventIterator.moveNext());

  }

  /**
   * Tests if concurrent insertions and removals work without errors and if the
   * result store is correct.
   * 
   * This test populates the event store with thousands of events. After that,
   * it creates a collection of remover threads and a collection of inserter
   * threads.
   * 
   * After the setup phase, it spawns the threads. The test successfully ends if
   * all prior existing events are removed and all new events are inserted
   * without concurrent access errors.
   * 
   * @throws InterruptedException
   */
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

    runAndWaitForThreads(workers);
    eventIterator = eventStore.query(EventType.TYPE_1.toString(), Long.MIN_VALUE, Long.MAX_VALUE);

    int eventsCount = 0;
    while (eventIterator.moveNext()) {
      ++eventsCount;
    }

    assertEquals(numInserters * eventsPerInserter, eventsCount);
  }

  /**
   * Tests if concurrent query and removeAll work without errors and if the
   * result store is correct.
   * 
   * This test populates the event store with thousands of events. After that,
   * it creates a thread that will remove all events of a type and a thread that
   * will query the event store.
   * 
   * The test successfully ends if the data view received by the query thread is
   * not affected by the removeAll called by the other thread.
   * 
   * @throws InterruptedException
   */
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
