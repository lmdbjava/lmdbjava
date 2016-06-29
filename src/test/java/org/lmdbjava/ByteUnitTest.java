package org.lmdbjava;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ByteUnitTest {

  @Test
  public void toBytes() {
    assertThat(ByteUnit.BYTES.toBytes(1), is(1L));
    assertThat(ByteUnit.KIBIBYTES.toBytes(1), is(1024L));
    assertThat(ByteUnit.MEBIBYTES.toBytes(1), is(1048576L));
    assertThat(ByteUnit.GIBIBYTES.toBytes(1), is(1073741824L));
    assertThat(ByteUnit.TEBIBYTES.toBytes(1), is(1099511627776L));
    assertThat(ByteUnit.PEBIBYTES.toBytes(1), is(1125899906842624L));
  }
}
