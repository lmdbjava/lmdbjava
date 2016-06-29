package org.lmdbjava;

import org.junit.Test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;

public class ByteUnitTest {

  @Test public void convertInternal() {
    for (long s = 0; s < 999; s++) {
      assertEquals(s / 1_024L, ByteUnit.BYTES.toKibibytes(s));
      assertEquals(s / 1_048_576L, ByteUnit.BYTES.toMebibytes(s));
      assertEquals(s / 1_073_741_824L, ByteUnit.BYTES.toGibibytes(s));
      assertEquals(s / 1_099_511_627_776L, ByteUnit.BYTES.toTebibytes(s));
      assertEquals(s / 1_125_899_906_842_624L, ByteUnit.BYTES.toPebibytes(s));

      assertEquals(s * 1_024L, ByteUnit.KIBIBYTES.toBytes(s));
      assertEquals(s / 1_024L, ByteUnit.KIBIBYTES.toMebibytes(s));
      assertEquals(s / 1_048_576L, ByteUnit.KIBIBYTES.toGibibytes(s));
      assertEquals(s / 1_073_741_824L, ByteUnit.KIBIBYTES.toTebibytes(s));
      assertEquals(s / 1_099_511_627_776L, ByteUnit.KIBIBYTES.toPebibytes(s));

      assertEquals(s * 1_048_576L, ByteUnit.MEBIBYTES.toBytes(s));
      assertEquals(s * 1_024L, ByteUnit.MEBIBYTES.toKibibytes(s));
      assertEquals(s / 1_024L, ByteUnit.MEBIBYTES.toGibibytes(s));
      assertEquals(s / 1_048_576L, ByteUnit.MEBIBYTES.toTebibytes(s));
      assertEquals(s / 1_073_741_824L, ByteUnit.MEBIBYTES.toPebibytes(s));

      assertEquals(s * 1_073_741_824L, ByteUnit.GIBIBYTES.toBytes(s));
      assertEquals(s * 1_048_576L, ByteUnit.GIBIBYTES.toKibibytes(s));
      assertEquals(s * 1_024L, ByteUnit.GIBIBYTES.toMebibytes(s));
      assertEquals(s / 1_024L, ByteUnit.GIBIBYTES.toTebibytes(s));
      assertEquals(s / 1_048_576L, ByteUnit.GIBIBYTES.toPebibytes(s));

      assertEquals(s * 1_099_511_627_776L, ByteUnit.TEBIBYTES.toBytes(s));
      assertEquals(s * 1_073_741_824L, ByteUnit.TEBIBYTES.toKibibytes(s));
      assertEquals(s * 1_048_576L, ByteUnit.TEBIBYTES.toMebibytes(s));
      assertEquals(s * 1_024L, ByteUnit.TEBIBYTES.toGibibytes(s));
      assertEquals(s / 1_024L, ByteUnit.TEBIBYTES.toPebibytes(s));

      assertEquals(s * 1_125_899_906_842_624L, ByteUnit.PEBIBYTES.toBytes(s));
      assertEquals(s * 1_099_511_627_776L, ByteUnit.PEBIBYTES.toKibibytes(s));
      assertEquals(s * 1_073_741_824L, ByteUnit.PEBIBYTES.toMebibytes(s));
      assertEquals(s * 1_048_576L, ByteUnit.PEBIBYTES.toGibibytes(s));
      assertEquals(s * 1_024L, ByteUnit.PEBIBYTES.toTebibytes(s));
    }
  }

  @Test public void format() {
    assertEquals("0 B", ByteUnit.format(0));
    assertEquals("1 B", ByteUnit.format(1));
    assertEquals("1 KiB", ByteUnit.format(1024));
    assertEquals("1 KiB", ByteUnit.format(1025));
    assertEquals("16 KiB", ByteUnit.format(16_384));
    assertEquals("1.2 MiB", ByteUnit.format(1_234_567));
    assertEquals("1.2 GiB", ByteUnit.format(1_288_490_189));
    assertEquals("8,192 PiB", ByteUnit.format(Long.MAX_VALUE));
  }

  @Test public void formatWithPattern() {
    String pattern = "0.0#";
    assertEquals("0.0 B", ByteUnit.format(0, pattern));
    assertEquals("1.0 B", ByteUnit.format(1, pattern));
    assertEquals("1.0 KiB", ByteUnit.format(1024, pattern));
    assertEquals("1.0 KiB", ByteUnit.format(1025, pattern));
    assertEquals("16.0 KiB", ByteUnit.format(16_384, pattern));
    assertEquals("1.18 MiB", ByteUnit.format(1_234_567, pattern));
  }

  @Test public void formatWithDecimalFormat() {
    NumberFormat format = new DecimalFormat("#.##", new DecimalFormatSymbols(Locale.FRENCH));
    assertEquals("16 KiB", ByteUnit.format(16_384, format));
    assertEquals("1,18 MiB", ByteUnit.format(1_234_567, format));
  }

  @Test public void formatNegativeValuesThrows() {
    try {
      ByteUnit.format(-1);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("bytes < 0: -1", e.getMessage());
    }
    try {
      ByteUnit.format(-1, "#.##");
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("bytes < 0: -1", e.getMessage());
    }
    NumberFormat format = new DecimalFormat("#.##", new DecimalFormatSymbols(Locale.FRENCH));
    try {
      ByteUnit.format(-1, format);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("bytes < 0: -1", e.getMessage());
    }
  }
}
