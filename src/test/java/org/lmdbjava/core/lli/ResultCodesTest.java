package org.lmdbjava.core.lli;

import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.lmdbjava.core.lli.ReturnCodes.*;
import static org.lmdbjava.core.lli.ReturnCodes.MDB_CURSOR_FULL;
import static org.lmdbjava.core.lli.ReturnCodes.checkRc;
import static org.lmdbjava.core.lli.ReturnCodes.rcException;

import org.junit.Test;


public class ResultCodesTest {

  private static final Set<Integer> RESULT_CODES = new HashSet<>();

  static {
    RESULT_CODES.add(MDB_BAD_DBI);
    RESULT_CODES.add(MDB_BAD_RSLOT);
    RESULT_CODES.add(MDB_BAD_TXN);
    RESULT_CODES.add(MDB_BAD_VALSIZE);
    RESULT_CODES.add(MDB_CORRUPTED);
    RESULT_CODES.add(MDB_CURSOR_FULL);
    RESULT_CODES.add(MDB_DBS_FULL);
    RESULT_CODES.add(MDB_INCOMPATIBLE);
    RESULT_CODES.add(MDB_INVALID);
    RESULT_CODES.add(MDB_KEYEXIST);
    RESULT_CODES.add(MDB_MAP_FULL);
    RESULT_CODES.add(MDB_MAP_RESIZED);
    RESULT_CODES.add(MDB_NOTFOUND);
    RESULT_CODES.add(MDB_PAGE_FULL);
    RESULT_CODES.add(MDB_PAGE_NOTFOUND);
    RESULT_CODES.add(MDB_PANIC);
    RESULT_CODES.add(MDB_READERS_FULL);
    RESULT_CODES.add(MDB_TLS_FULL);
    RESULT_CODES.add(MDB_TXN_FULL);
    RESULT_CODES.add(MDB_VERSION_MISMATCH);
  }

  @Test
  public void checkErrAll() throws Exception {
    for (final Integer rc : RESULT_CODES) {
      try {
        checkRc(rc);
        fail("Exception expected for RC " + rc);
      } catch (LmdbNativeException e) {
        assertThat(e.getReturnCode(), is(rc));
      }
    }
  }

  @Test
  public void checkErrConstantDerived() {
    try {
      checkRc(20);
      fail("should throw");
    } catch (ConstantDerviedException e) {
      assertThat(e.getReturnCode(), is(20));
      assertThat(e.getMessage(), is("Platform constant error code: ENOTDIR"));
    }
  }

  @Test
  public void checkErrCursorFull() throws Exception {
    try {
      checkRc(MDB_CURSOR_FULL);
      fail("should throw");
    } catch (LmdbNativeException e) {
      assertThat(e.getReturnCode(), is(MDB_CURSOR_FULL));
      assertThat(e.getMessage(), is("MDB_CURSOR_FULL: Internal error - cursor stack limit reached"));
    }
  }

  @Test
  public void mapperReturnsUnique() {
    final Set<LmdbNativeException> seen = new HashSet<>();
    for (final Integer rc : RESULT_CODES) {
      LmdbNativeException ex = rcException(rc);
      assertThat(ex, is(notNullValue()));
      seen.add(ex);
    }
    assertThat(seen.size(), is(RESULT_CODES.size()));
  }

}
