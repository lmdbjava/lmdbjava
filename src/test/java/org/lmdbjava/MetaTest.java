package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import org.lmdbjava.Meta.Version;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

public class MetaTest {

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(Meta.class);
  }

  @Test
  public void version() throws Exception {
    final Version v = Meta.version();
    assertThat(v, not(nullValue()));
    assertThat(v.major, is(0));
    assertThat(v.minor, is(9));
  }
}
