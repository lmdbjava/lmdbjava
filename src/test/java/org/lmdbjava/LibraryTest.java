package org.lmdbjava;

import static java.lang.Long.BYTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import org.lmdbjava.Library.MDB_val;
import static org.lmdbjava.Library.runtime;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

public class LibraryTest {

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(Library.class);
  }

  @Test
  public void structureFieldOrder() throws Exception {
    MDB_val v = new MDB_val(runtime);
    assertThat(v.size.offset(), is(0L));
    assertThat(v.data.offset(), is((long) BYTES));
  }
}
