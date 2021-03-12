/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2021 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import static java.lang.Integer.MAX_VALUE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.fail;
import static org.lmdbjava.Cursor.FullException.MDB_CURSOR_FULL;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.TestUtils.invokePrivateConstructor;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.lmdbjava.Cursor.FullException;
import org.lmdbjava.Dbi.BadDbiException;
import org.lmdbjava.Dbi.BadValueSizeException;
import org.lmdbjava.Dbi.DbFullException;
import org.lmdbjava.Dbi.IncompatibleException;
import org.lmdbjava.Dbi.KeyExistsException;
import org.lmdbjava.Dbi.KeyNotFoundException;
import org.lmdbjava.Dbi.MapResizedException;
import org.lmdbjava.Env.FileInvalidException;
import org.lmdbjava.Env.MapFullException;
import org.lmdbjava.Env.ReadersFullException;
import org.lmdbjava.Env.VersionMismatchException;
import org.lmdbjava.LmdbNativeException.ConstantDerivedException;
import org.lmdbjava.LmdbNativeException.PageCorruptedException;
import org.lmdbjava.LmdbNativeException.PageFullException;
import org.lmdbjava.LmdbNativeException.PageNotFoundException;
import org.lmdbjava.LmdbNativeException.PanicException;
import org.lmdbjava.LmdbNativeException.TlsFullException;
import org.lmdbjava.Txn.BadException;
import org.lmdbjava.Txn.BadReaderLockException;
import org.lmdbjava.Txn.TxFullException;

/**
 * Test {@link ResultCodeMapper} and {@link LmdbException}.
 */
public final class ResultCodeMapperTest {

  private static final Set<LmdbNativeException> EXCEPTIONS = new HashSet<>();
  private static final Set<Integer> RESULT_CODES = new HashSet<>();

  static {
    // separate collection instances used to simplify duplicate RC detection
    EXCEPTIONS.add(new BadDbiException());
    EXCEPTIONS.add(new BadReaderLockException());
    EXCEPTIONS.add(new BadException());
    EXCEPTIONS.add(new BadValueSizeException());
    EXCEPTIONS.add(new PageCorruptedException());
    EXCEPTIONS.add(new FullException());
    EXCEPTIONS.add(new DbFullException());
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
    EXCEPTIONS.add(new TxFullException());
    EXCEPTIONS.add(new VersionMismatchException());

    for (final LmdbNativeException e : EXCEPTIONS) {
      RESULT_CODES.add(e.getResultCode());
    }
  }

  @Test
  public void checkErrAll() {
    for (final Integer rc : RESULT_CODES) {
      try {
        checkRc(rc);
        fail("Exception expected for RC " + rc);
      } catch (final LmdbNativeException e) {
        assertThat(e.getResultCode(), is(rc));
      }
    }
  }

  @Test(expected = ConstantDerivedException.class)
  public void checkErrConstantDerived() {
    checkRc(20);
  }

  @Test
  public void checkErrConstantDerivedMessage() {
    try {
      checkRc(2);
      fail("Should have raised exception");
    } catch (final ConstantDerivedException ex) {
      assertThat(ex.getMessage(), containsString("No such file or directory"));
    }
  }

  @Test(expected = FullException.class)
  public void checkErrCursorFull() {
    checkRc(MDB_CURSOR_FULL);
  }

  @Test(expected = IllegalArgumentException.class)
  public void checkErrUnknownResultCode() {
    checkRc(MAX_VALUE);
  }

  @Test
  public void coverPrivateConstructors() {
    invokePrivateConstructor(ResultCodeMapper.class);
  }

  @Test
  public void lmdbExceptionPreservesRootCause() {
    final Exception cause = new IllegalStateException("root cause");
    final LmdbException e = new LmdbException("test", cause);
    assertThat(e.getCause(), is(cause));
    assertThat(e.getMessage(), is("test"));
  }

  @Test
  public void mapperReturnsUnique() {
    final Set<LmdbNativeException> seen = new HashSet<>();
    for (final Integer rc : RESULT_CODES) {
      try {
        checkRc(rc);
      } catch (final LmdbNativeException ex) {
        assertThat(ex, is(notNullValue()));
        seen.add(ex);
      }
    }
    assertThat(seen, hasSize(RESULT_CODES.size()));
  }

  @Test
  public void noDuplicateResultCodes() {
    assertThat(RESULT_CODES.size(), is(EXCEPTIONS.size()));
  }

}
