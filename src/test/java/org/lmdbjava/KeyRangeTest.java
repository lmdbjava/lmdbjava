/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
import static org.lmdbjava.KeyRangeType.IteratorOp.TERMINATE;

import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.lmdbjava.KeyRangeType.CursorOp;
import org.lmdbjava.KeyRangeType.IteratorOp;

/**
 * Test {@link KeyRange}.
 *
 * <p>This test case focuses on the contractual correctness detailed in {@link KeyRangeType}. It
 * does this using integers as per the JavaDoc examples.
 */
public final class KeyRangeTest {

  private final FakeCursor cursor = new FakeCursor();

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
    verify(atLeastBackward(5), 4, 2);
    verify(atLeastBackward(6), 6, 4, 2);
    verify(atLeastBackward(9), 8, 6, 4, 2);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(5), 6, 8);
    verify(atLeast(6), 6, 8);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(5), 8, 6);
    verify(atMostBackward(6), 8, 6);
  }

  @Test
  public void atMostTest() {
    verify(atMost(5), 2, 4);
    verify(atMost(6), 2, 4, 6);
  }

  @Before
  public void before() {
    cursor.reset();
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(7, 3), 6, 4);
    verify(closedBackward(6, 2), 6, 4, 2);
    verify(closedBackward(9, 3), 8, 6, 4);
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(8, 3), 8, 6, 4);
    verify(closedOpenBackward(7, 2), 6, 4);
    verify(closedOpenBackward(9, 3), 8, 6, 4);
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(3, 8), 4, 6);
    verify(closedOpen(2, 6), 2, 4);
  }

  @Test
  public void closedTest() {
    verify(closed(3, 7), 4, 6);
    verify(closed(2, 6), 2, 4, 6);
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
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(6), 4, 2);
    verify(greaterThanBackward(7), 6, 4, 2);
    verify(greaterThanBackward(9), 8, 6, 4, 2);
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(4), 6, 8);
    verify(greaterThan(3), 4, 6, 8);
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(5), 8, 6);
    verify(lessThanBackward(2), 8, 6, 4);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(5), 2, 4);
    verify(lessThan(8), 2, 4, 6);
  }

  @Test
  public void openBackwardTest() {
    verify(openBackward(7, 2), 6, 4);
    verify(openBackward(8, 1), 6, 4, 2);
    verify(openBackward(9, 4), 8, 6);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(7, 2), 6, 4, 2);
    verify(openClosedBackward(8, 4), 6, 4);
    verify(openClosedBackward(9, 4), 8, 6, 4);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(3, 8), 4, 6, 8);
    verify(openClosed(2, 6), 4, 6);
  }

  @Test
  public void openTest() {
    verify(open(3, 7), 4, 6);
    verify(open(2, 8), 4, 6);
  }

  private void verify(final KeyRange<Integer> range, final int... expected) {
    final List<Integer> results = new ArrayList<>();

    Integer buff = cursor.apply(range.getType().initialOp(), range.getStart());

    IteratorOp op;
    do {
      final Integer finalBuff = buff;
      final RangeComparator rangeComparator =
          new CursorIterable.JavaRangeComparator<>(range, Integer::compareTo, () -> finalBuff);
      op = range.getType().iteratorOp(buff, rangeComparator);
      switch (op) {
        case CALL_NEXT_OP:
          buff = cursor.apply(range.getType().nextOp(), range.getStart());
          break;
        case TERMINATE:
          break;
        case RELEASE:
          results.add(buff);
          buff = cursor.apply(range.getType().nextOp(), range.getStart());
          break;
        default:
          throw new IllegalStateException("Unknown operation");
      }
    } while (op != TERMINATE);

    for (int idx = 0; idx < results.size(); idx++) {
      assertThat("idx " + idx, results.get(idx), is(expected[idx]));
    }
    assertThat(results.size(), is(expected.length));
  }

  /**
   * Cursor that behaves like an LMDB cursor would.
   *
   * <p>We use <code>Integer</code> rather than the primitive to represent a <code>null</code>
   * buffer.
   */
  private static final class FakeCursor {

    private static final int[] KEYS = new int[] {2, 4, 6, 8};
    private int position;

    Integer apply(final CursorOp op, final Integer startKey) {
      final Integer key;
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
        case GET_START_KEY_BACKWARD:
          key = getWithSetRange(startKey);
          if (key != null) {
            return key;
          }
          return last();
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
