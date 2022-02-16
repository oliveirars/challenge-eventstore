package net.intelie.challenges.util;

import java.util.Objects;

/**
 * Utility class. Provides general purpose methods (i.e. validation methods).
 */
public class Utils {

  /**
   * Checks if an object is <code>null</code>. In affirmative case, throws a new
   * instance of {@link IllegalArgumentException}. This method is an alternative
   * to {@link Objects#requireNonNull} which throws a
   * {@link NullPointerException}.
   * 
   * @param obj The object to be checked.
   * @param msg The message to be used to construct the exception.
   */
  public static void requireNonNull(Object obj, String msg) {
    if (obj == null) {
      throw new IllegalArgumentException(msg);
    }
  }
}