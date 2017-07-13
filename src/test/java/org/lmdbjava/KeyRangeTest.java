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

import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.lmdbjava.KeyRange.CursorOp;
import static org.lmdbjava.KeyRange.CursorOp.FIRST;
import org.lmdbjava.KeyRange.IteratorOp;
import static org.lmdbjava.KeyRange.IteratorOp.TERMINATE;
import static org.lmdbjava.KeyRange.atLeast;
import static org.lmdbjava.KeyRange.atLeastBackward;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.KeyRange.atMostBackward;
import static org.lmdbjava.KeyRange.backward;
import static org.lmdbjava.KeyRange.forward;
import static org.lmdbjava.KeyRange.range;
import static org.lmdbjava.KeyRange.rangeBackward;

/**
 * Test {@link KeyRange}.
 *
 * <p>
 * This test case focuses on the contractual correctness detailed in
 * {@link KeyRangeType}. It does this using integers as per the JavaDoc
 * examples.
 */
public final class KeyRangeTest {

  private final FakeCursor cursor = new FakeCursor();

  @Test
  public void backwardRange() {
    verify(rangeBackward(7, 3), 6, 4);
    verify(rangeBackward(6, 2), 6, 4, 2);
  }

  @Test
  public void backwardStart() {
    verify(atLeastBackward(5), 4, 2);
    verify(atLeastBackward(6), 6, 4, 2);
  }

  @Test
  public void backwardStop() {
    verify(atMostBackward(5), 8, 6);
    verify(atMostBackward(6), 8, 6);
  }

  @Test
  public void backwardTest() {
    verify(backward(), 8, 6, 4, 2);
  }

  @Before
  public void before() {
    cursor.reset();
  }

  @Test
  public void fakeCursor() {
    assertThat(cursor.first(), is(2));
    assertThat(cursor.next(), is(4));
    assertThat(cursor.next(), is(6));
    assertThat(cursor.next(), is(8));
    assertThat(cursor.next(), nullValue());
    assertThat(cursor.first(), is(2));
    assertThat(cursor.prev(), nullValue());
    assertThat(cursor.getWithSetRange(3), is(4));
    assertThat(cursor.next(), is(6));
    assertThat(cursor.getWithSetRange(1), is(2));
    assertThat(cursor.last(), is(8));
    assertThat(cursor.getWithSetRange(100), nullValue());
  }

  @Test
  public void forwardRange() {
    verify(range(3, 7), 4, 6);
    verify(range(2, 6), 2, 4, 6);
  }

  @Test
  public void forwardStart() {
    verify(atLeast(5), 6, 8);
    verify(atLeast(6), 6, 8);
  }

  @Test
  public void forwardStop() {
    verify(atMost(5), 2, 4);
    verify(atMost(6), 2, 4, 6);
  }

  @Test
  public void forwardTest() {
    verify(forward(), 2, 4, 6, 8);
  }

  private void verify(final KeyRange<Integer> range, final int... expected) {
    final List<Integer> results = new ArrayList<>();

    final Integer s = range.getStart();
    Integer buff = cursor.apply(range.initialOp(), s);

    IteratorOp op;
    do {
      op = range.iteratorOp(Integer::compare, buff);
      switch (op) {
        case CALL_NEXT_OP:
          buff = cursor.apply(range.nextOp(), s);
          break;
        case TERMINATE:
          break;
        case RELEASE:
          results.add(buff);
          buff = cursor.apply(range.nextOp(), s);
          break;
        default:
          throw new IllegalStateException("Unknown operation");
      }
    } while (op != TERMINATE);

    assertThat(results.size(), is(expected.length));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expected[idx]));
    }

  }

  /**
   * Cursor that behaves like an LMDB cursor would.
   *
   * <p>
   * We use <code>Integer</code> rather than the primitive to represent a
   * <code>null</code> buffer.
   */
  private static class FakeCursor {

    private static final int[] KEYS = new int[]{2, 4, 6, 8};
    private int position;

    @SuppressWarnings("checkstyle:ReturnCount")
    Integer apply(final CursorOp op, final Integer startKey) {
      switch (op) {
        case FIRST:
          return first();
        case LAST:
          return last();
        case NEXT:
          return next();
        case PREV:
          return prev();
        case GET_START_KEY:
          return getWithSetRange(startKey);
        default:
          throw new IllegalStateException("Unknown operation");
      }
    }

    Integer first() {
      position = 0;
      return KEYS[position];
    }

    Integer getWithSetRange(final Integer startKey) {
      for (int idx = 0; idx < KEYS.length; idx++) {
        final int candidate = KEYS[idx];
        if (candidate == startKey || candidate > startKey) {
          position = idx;
          return KEYS[position];
        }
      }
      return null;
    }

    Integer last() {
      position = KEYS.length - 1;
      return KEYS[position];
    }

    Integer next() {
      position += 1;
      if (position == KEYS.length) {
        return null;
      }
      return KEYS[position];
    }

    Integer prev() {
      position -= 1;
      if (position == -1) {
        return null;
      }
      return KEYS[position];
    }

    void reset() {
      position = 0;
    }

  }

}
