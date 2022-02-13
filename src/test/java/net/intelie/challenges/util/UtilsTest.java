package net.intelie.challenges.util;

import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class UtilsTest {

  @Test
  public void requireNonNull_ShouldThrowIllegalArgument_When_ArgumentIsNull() {
    String msg = "Exception message";
    assertThrows(msg, IllegalArgumentException.class, () -> Utils.requireNonNull(null, msg));
  }

  @Test(expected = Test.None.class)
  public void requireNonNull_ShouldNotThrowIllegalArgument_When_ArgumentIsNotNull() {
    Utils.requireNonNull("Test Object", "Exception message");
  }

}
