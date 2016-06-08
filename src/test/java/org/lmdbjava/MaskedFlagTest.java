package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Test;
import static org.lmdbjava.EnvFlags.MDB_FIXEDMAP;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_RDONLY;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;

public class MaskedFlagTest {

  @Test
  public void isSetOperates() {
    assertThat(isSet(0, MDB_NOSYNC), is(false));
    assertThat(isSet(0, MDB_FIXEDMAP), is(false));
    assertThat(isSet(0, MDB_RDONLY), is(false));

    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_NOSYNC), is(false));
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_FIXEDMAP), is(true));
    assertThat(isSet(MDB_FIXEDMAP.getMask(), MDB_RDONLY), is(false));

    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_NOSYNC), is(true));
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_FIXEDMAP), is(false));
    assertThat(isSet(MDB_NOSYNC.getMask(), MDB_RDONLY), is(false));

    final int syncFixed = mask(MDB_NOSYNC, MDB_FIXEDMAP);
    assertThat(isSet(syncFixed, MDB_NOSYNC), is(true));
    assertThat(isSet(syncFixed, MDB_FIXEDMAP), is(true));
    assertThat(isSet(syncFixed, MDB_RDONLY), is(false));
  }

  @Test
  public void masking() {
    final EnvFlags[] nullFlags = null;
    assertThat(mask(nullFlags), is(0));

    final EnvFlags[] emptyFlags = new EnvFlags[]{};
    assertThat(mask(emptyFlags), is(0));

    assertThat(mask(MDB_NOSYNC), is(MDB_NOSYNC.getMask()));

    final int expected = MDB_NOSYNC.getMask() + MDB_FIXEDMAP.getMask();
    assertThat(mask(MDB_NOSYNC, MDB_FIXEDMAP), is(expected));
  }
}
