package org.lmdbjava;

import static java.lang.Integer.MAX_VALUE;
import java.util.HashSet;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import org.junit.Test;
import static org.lmdbjava.CursorFullException.MDB_CURSOR_FULL;
import org.lmdbjava.Database.BadDbiException;
import org.lmdbjava.Database.BadValueSizeException;
import org.lmdbjava.Database.IncompatibleException;
import org.lmdbjava.Database.KeyExistsException;
import org.lmdbjava.Database.KeyNotFoundException;
import org.lmdbjava.Database.MapResizedException;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Env.ReadersFullException;
import org.lmdbjava.Env.VersionMismatchException;
import static org.lmdbjava.ResultCodeMapper.MDB_SUCCESS;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.ResultCodeMapper.rcException;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;
import org.lmdbjava.Txn.BadException;
import org.lmdbjava.Txn.BadReaderLockException;
import org.lmdbjava.Txn.FullException;

public class ResultCodeMapperTest {

  private static final Set<LmdbNativeException> EXCEPTIONS = new HashSet<>();
  private static final Set<Integer> RESULT_CODES = new HashSet<>();

  static {
    // separate collection instances used to simplify duplicate RC detection
    EXCEPTIONS.add(new BadDbiException());
    EXCEPTIONS.add(new BadReaderLockException());
    EXCEPTIONS.add(new BadException());
    EXCEPTIONS.add(new BadValueSizeException());
    EXCEPTIONS.add(new PageCorruptedException());
    EXCEPTIONS.add(new CursorFullException());
    EXCEPTIONS.add(new DatabasesFullException());
    EXCEPTIONS.add(new IncompatibleException());
    EXCEPTIONS.add(new FileInvalidException());
    EXCEPTIONS.add(new KeyExistsException());
    EXCEPTIONS.add(new MapFullException());
    EXCEPTIONS.add(new MapResizedException());
    EXCEPTIONS.add(new KeyNotFoundException());
    EXCEPTIONS.add(new PageFullException());
    EXCEPTIONS.add(new PageNotFoundException());
    EXCEPTIONS.add(new PanicException());
    EXCEPTIONS.add(new ReadersFullException());
    EXCEPTIONS.add(new TlsFullException());
    EXCEPTIONS.add(new FullException());
    EXCEPTIONS.add(new VersionMismatchException());

    for (LmdbNativeException e : EXCEPTIONS) {
      RESULT_CODES.add(e.getResultCode());
    }
  }

  @Test
  public void checkErrAll() throws Exception {
    for (final Integer rc : RESULT_CODES) {
      try {
        checkRc(rc);
        fail("Exception expected for RC " + rc);
      } catch (LmdbNativeException e) {
        assertThat(e.getResultCode(), is(rc));
      }
    }
  }

  @Test(expected = ConstantDerviedException.class)
  public void checkErrConstantDerived() throws Exception {
    checkRc(20);
  }

  @Test(expected = CursorFullException.class)
  public void checkErrCursorFull() throws Exception {
    checkRc(MDB_CURSOR_FULL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkErrUnknownResultCode() throws Exception {
    checkRc(MAX_VALUE);
  }

  @Test(expected = IllegalArgumentException.class)
  @SuppressWarnings("ThrowableResultIgnored")
  public void checkSuccessRaisesErrorIfPassedToRcException() throws Exception {
    rcException(MDB_SUCCESS);
  }

  @Test
  public void coverPrivateConstructors() throws Exception {
    invokePrivateConstructor(ResultCodeMapper.class);
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

  @Test
  public void noDuplicateResultCodes() {
    assertThat(RESULT_CODES.size(), is(EXCEPTIONS.size()));
  }

}
