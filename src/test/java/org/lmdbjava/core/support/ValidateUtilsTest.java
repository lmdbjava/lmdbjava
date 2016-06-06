package org.lmdbjava.core.support;

import org.junit.Test;
import static org.lmdbjava.core.support.Validate.hasLength;
import static org.lmdbjava.core.support.Validate.notNull;

public class ValidateUtilsTest {

  @Test
  public void hasLengthPass() {
    hasLength("hello world");
  }

  @Test(expected = IllegalArgumentException.class)
  public void hasLengthWhenEmpty() {
    hasLength("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void hasLengthWhenNull() {
    hasLength(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullCheckFail() {
    notNull(null);
  }

  public void nullCheckPass() {
    notNull("hello world");
  }
}
