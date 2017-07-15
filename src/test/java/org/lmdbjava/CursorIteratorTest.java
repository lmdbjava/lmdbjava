/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2017 The LmdbJava Open Source Project
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import static java.util.Arrays.asList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CursorIterator.IteratorType.BACKWARD;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import org.lmdbjava.CursorIterator.KeyVal;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.KeyRange.all;
import static org.lmdbjava.KeyRange.allBackward;
import static org.lmdbjava.KeyRange.atLeast;
import static org.lmdbjava.KeyRange.atLeastBackward;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.KeyRange.atMostBackward;
import static org.lmdbjava.KeyRange.closed;
import static org.lmdbjava.KeyRange.closedBackward;
import static org.lmdbjava.KeyRange.closedOpen;
import static org.lmdbjava.KeyRange.closedOpenBackward;
import static org.lmdbjava.KeyRange.greaterThan;
import static org.lmdbjava.KeyRange.greaterThanBackward;
import static org.lmdbjava.KeyRange.lessThan;
import static org.lmdbjava.KeyRange.lessThanBackward;
import static org.lmdbjava.KeyRange.open;
import static org.lmdbjava.KeyRange.openBackward;
import static org.lmdbjava.KeyRange.openClosed;
import static org.lmdbjava.KeyRange.openClosedBackward;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.bb;

/**
 * Test {@link CursorIterator}.
 */
public final class CursorIteratorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<ByteBuffer> db;
  private Env<ByteBuffer> env;
  private Deque<Integer> list;

  @After
  public void after() {
    env.close();
  }

  @Test
  public void allBackwardTest() {
    verify(allBackward(), 8, 6, 4, 2);
  }

  @Test
  public void allTest() {
    verify(all(), 2, 4, 6, 8);
  }

  @Test
  public void atLeastBackwardTest() {
    verify(atLeastBackward(bb(5)), 4, 2);
    verify(atLeastBackward(bb(6)), 6, 4, 2);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(bb(5)), 6, 8);
    verify(atLeast(bb(6)), 6, 8);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(bb(5)), 8, 6);
    verify(atMostBackward(bb(6)), 8, 6);
  }

  @Test
  public void atMostTest() {
    verify(atMost(bb(5)), 2, 4);
    verify(atMost(bb(6)), 2, 4, 6);
  }

  @Test
  public void backwardDeprecated() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn, BACKWARD)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.val().getInt(), is(list.pollLast()));
        assertThat(kv.key().getInt(), is(list.pollLast()));
        assertThat(c.hasNext(), is(list.peekFirst() != null));
      }
    }
  }

  @Test
  public void backwardSeekDeprecated() {
    final ByteBuffer key = bb(6);
    list.pollLast();
    list.pollLast();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn, key, BACKWARD)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.val().getInt(), is(list.pollLast()));
        assertThat(kv.key().getInt(), is(list.pollLast()));
      }
    }
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = open(path, 10, MDB_NOSUBDIR);
    db = env.openDbi(DB_1, MDB_CREATE);
    list = new LinkedList<>();
    list.addAll(asList(2, 3, 4, 5, 6, 7, 8, 9));
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);
      c.put(bb(2), bb(3), MDB_NOOVERWRITE);
      c.put(bb(4), bb(5));
      c.put(bb(6), bb(7));
      c.put(bb(8), bb(9));
      txn.commit();
    }
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(bb(7), bb(3)), 6, 4);
    verify(closedBackward(bb(6), bb(2)), 6, 4, 2);
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(bb(8), bb(3)), 8, 6, 4);
    verify(closedOpenBackward(bb(7), bb(2)), 6, 4);
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(bb(3), bb(8)), 4, 6);
    verify(closedOpen(bb(2), bb(6)), 2, 4);
  }

  @Test
  public void closedTest() {
    verify(closed(bb(3), bb(7)), 4, 6);
    verify(closed(bb(2), bb(6)), 2, 4, 6);
  }

  @Test
  public void forwardDeprecated() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn, FORWARD)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.key().getInt(), is(list.pollFirst()));
        assertThat(kv.val().getInt(), is(list.pollFirst()));
        assertThat(c.hasNext(), is(list.peekFirst() != null));
      }
    }
  }

  @Test
  public void forwardSeekDeprecated() {
    final ByteBuffer key = bb(4);
    list.pollFirst();
    list.pollFirst();

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn, key, FORWARD)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.key().getInt(), is(list.pollFirst()));
        assertThat(kv.val().getInt(), is(list.pollFirst()));
      }
    }
  }

  @Test
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(bb(6)), 4, 2);
    verify(greaterThanBackward(bb(7)), 6, 4, 2);
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(bb(4)), 6, 8);
    verify(greaterThan(bb(3)), 4, 6, 8);
  }

  @Test
  public void iterate() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.key().getInt(), is(list.pollFirst()));
        assertThat(kv.val().getInt(), is(list.pollFirst()));
      }
    }
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(bb(5)), 8, 6);
    verify(lessThanBackward(bb(2)), 8, 6, 4);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(bb(5)), 2, 4);
    verify(lessThan(bb(8)), 2, 4, 6);
  }

  @Test(expected = NoSuchElementException.class)
  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        assertThat(kv.key().getInt(), is(list.pollFirst()));
        assertThat(kv.val().getInt(), is(list.pollFirst()));
      }
      assertThat(c.hasNext(), is(false));
      c.next();
    }
  }

  @Test
  public void openBackwardTest() {
    verify(openBackward(bb(7), bb(2)), 6, 4);
    verify(openBackward(bb(8), bb(1)), 6, 4, 2);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(bb(7), bb(2)), 6, 4, 2);
    verify(openClosedBackward(bb(8), bb(4)), 6, 4);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(bb(3), bb(8)), 4, 6, 8);
    verify(openClosed(bb(2), bb(6)), 4, 6);
  }

  @Test
  public void openTest() {
    verify(open(bb(3), bb(7)), 4, 6);
    verify(open(bb(2), bb(8)), 4, 6);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void removeUnsupported() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn)) {
      c.remove();
    }
  }

  private void verify(final KeyRange<ByteBuffer> range, final int... expected) {
    final List<Integer> results = new ArrayList<>();

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterator<ByteBuffer> c = db.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c.iterable()) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        results.add(key);
        assertThat(val, is(key + 1));
      }
    }

    assertThat(results.size(), is(expected.length));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expected[idx]));
    }
  }

}
