package net.intelie.challenges.util;

public class Utils {
  public static void requireNonNull(Object obj, String msg) {
    if (obj == null) {
      throw new IllegalArgumentException(msg);
    }
  }
}