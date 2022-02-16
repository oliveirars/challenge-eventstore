package net.intelie.challenges.util;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

/**
 * Unit tests for {@link Utils}.
 */
public class UtilsTest {

  /**
   * Tests if <code>requireNonNull</code> throws
   * {@link IllegalArgumentException} when called with <code>null</code> object.
   */
  @Test
  public void requireNonNull_ShouldThrowIllegalArgument_When_ArgumentIsNull() {
    String msg = "Exception message";
    assertThrows(msg, IllegalArgumentException.class, () -> Utils.requireNonNull(null, msg));
  }

  /**
   * Tests if <code>requireNonNull</code> does not throw
   * {@link IllegalArgumentException} when called with non <code>null</code>
   * object.
   */
  @Test(expected = Test.None.class)
  public void requireNonNull_ShouldNotThrowIllegalArgument_When_ArgumentIsNotNull() {
    Utils.requireNonNull("Test Object", "Exception message");
  }

}
