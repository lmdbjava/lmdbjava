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

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import io.netty.buffer.ByteBuf;
import java.io.File;
import java.io.IOException;
import static java.lang.Long.BYTES;
import java.nio.ByteBuffer;
import static java.nio.charset.StandardCharsets.UTF_8;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.GetOp.MDB_SET_KEY;
import static org.lmdbjava.GetOp.MDB_SET_RANGE;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_NEXT;
import static org.lmdbjava.SeekOp.MDB_PREV;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.mdb;
import static org.lmdbjava.TestUtils.nb;

/**
 * Test {@link Cursor} with different buffer implementations.
 */
@RunWith(Parameterized.class)
public final class CursorParamTest {

  /**
   * Injected by {@link #data()} with appropriate runner.
   */
  @Parameter
  public BufferRunner<?> runner;

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  @Parameters(name = "{index}: buffer adapter: {0}")
  public static Object[] data() {
    final BufferRunner<ByteBuffer> bb1 = new ByteBufferRunner(PROXY_OPTIMAL);
    final BufferRunner<ByteBuffer> bb2 = new ByteBufferRunner(PROXY_SAFE);
    final BufferRunner<byte[]> ba = new ByteArrayRunner(PROXY_BA);
    final BufferRunner<DirectBuffer> db = new DirectBufferRunner();
    final BufferRunner<ByteBuf> netty = new NettyBufferRunner();
    return new Object[]{bb1, bb2, ba, db, netty};
  }

  @Test
  public void execute() {
    runner.execute(tmp);
  }

  /**
   * Abstract implementation of {@link BufferRunner}.
   *
   * @param <T> buffer type
   */
  private abstract static class AbstractBufferRunner<T> implements
      BufferRunner<T> {

    final BufferProxy<T> proxy;

    protected AbstractBufferRunner(final BufferProxy<T> proxy) {
      this.proxy = proxy;
    }

    @SuppressWarnings("checkstyle:executablestatementcount")
    @Override
    public final void execute(final TemporaryFolder tmp) {
      try (Env<T> env = env(tmp)) {
        assertThat(env.getDbiNames(), empty());
        final Dbi<T> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
        assertThat(env.getDbiNames().get(0), is(DB_1.getBytes(UTF_8)));
        try (Txn<T> txn = env.txnWrite()) {
          // populate data
          final Cursor<T> c = db.openCursor(txn);
          c.put(set(1), set(2), MDB_NOOVERWRITE);
          c.put(set(3), set(4));
          c.put(set(5), set(6));
          // we cannot set the value for ByteArrayProxy
          // but the key is still valid.
          final T valForKey7 = c.reserve(set(7), BYTES);
          set(valForKey7, 8);

          // check MDB_SET operations
          final T key3 = set(3);
          assertThat(c.get(key3, MDB_SET_KEY), is(true));
          assertThat(get(c.key()), is(3));
          assertThat(get(c.val()), is(4));
          final T key6 = set(6);
          assertThat(c.get(key6, MDB_SET_RANGE), is(true));
          assertThat(get(c.key()), is(7));
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(get(c.val()), is(8));
          }
          final T key999 = set(999);
          assertThat(c.get(key999, MDB_SET_KEY), is(false));

          // check MDB navigation operations
          assertThat(c.seek(MDB_LAST), is(true));
          final int mdb1 = get(c.key());
          final int mdb2 = get(c.val());

          assertThat(c.seek(MDB_PREV), is(true));
          final int mdb3 = get(c.key());
          final int mdb4 = get(c.val());

          assertThat(c.seek(MDB_NEXT), is(true));
          final int mdb5 = get(c.key());
          final int mdb6 = get(c.val());

          assertThat(c.seek(MDB_FIRST), is(true));
          final int mdb7 = get(c.key());
          final int mdb8 = get(c.val());

          // assert afterwards to ensure memory address from LMDB
          // are valid within same txn and across cursor movement
          // MDB_LAST
          assertThat(mdb1, is(7));
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(mdb2, is(8));
          }

          // MDB_PREV
          assertThat(mdb3, is(5));
          assertThat(mdb4, is(6));

          // MDB_NEXT
          assertThat(mdb5, is(7));
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(mdb6, is(8));
          }

          // MDB_FIRST
          assertThat(mdb7, is(1));
          assertThat(mdb8, is(2));
        }
      }
    }

    private Env<T> env(final TemporaryFolder tmp) {
      try {
        final File path = tmp.newFile();
        return create(proxy)
            .setMapSize(KIBIBYTES.toBytes(1_024))
            .setMaxReaders(1)
            .setMaxDbs(1)
            .open(path, POSIX_MODE, MDB_NOSUBDIR);
      } catch (final IOException e) {
        throw new LmdbException("IO failure", e);
      }
    }

  }

  /**
   * {@link BufferRunner} for Java byte buffers.
   */
  private static class ByteArrayRunner extends AbstractBufferRunner<byte[]> {

    ByteArrayRunner(final BufferProxy<byte[]> proxy) {
      super(proxy);
    }

    @Override
    public int get(final byte[] buff) {
      return (buff[0] & 0xFF) << 24
                 | (buff[1] & 0xFF) << 16
                 | (buff[2] & 0xFF) << 8
                 | (buff[3] & 0xFF);
    }

    @Override
    public byte[] set(final int val) {
      final byte[] buff = new byte[4];
      buff[0] = (byte) (val >>> 24);
      buff[1] = (byte) (val >>> 16);
      buff[2] = (byte) (val >>> 8);
      buff[3] = (byte) val;
      return buff;
    }

    @Override
    public void set(final byte[] buff, final int val) {
      buff[0] = (byte) (val >>> 24);
      buff[1] = (byte) (val >>> 16);
      buff[2] = (byte) (val >>> 8);
      buff[3] = (byte) val;
    }
  }

  /**
   * {@link BufferRunner} for Java byte buffers.
   */
  private static class ByteBufferRunner extends AbstractBufferRunner<ByteBuffer> {

    ByteBufferRunner(final BufferProxy<ByteBuffer> proxy) {
      super(proxy);
    }

    @Override
    public int get(final ByteBuffer buff) {
      return buff.getInt(0);
    }

    @Override
    public ByteBuffer set(final int val) {
      return bb(val);
    }

    @Override
    public void set(final ByteBuffer buff, final int val) {
      buff.putInt(val);
    }

  }

  /**
   * {@link BufferRunner} for Agrona direct buffer.
   */
  private static class DirectBufferRunner extends AbstractBufferRunner<DirectBuffer> {

    DirectBufferRunner() {
      super(PROXY_DB);
    }

    @Override
    public int get(final DirectBuffer buff) {
      return buff.getInt(0);
    }

    @Override
    public DirectBuffer set(final int val) {
      return mdb(val);
    }

    @Override
    public void set(final DirectBuffer buff, final int val) {
      ((MutableDirectBuffer) buff).putInt(0, val);
    }

  }

  /**
   * {@link BufferRunner} for Netty byte buf.
   */
  private static class NettyBufferRunner extends AbstractBufferRunner<ByteBuf> {

    NettyBufferRunner() {
      super(new ByteBufProxy());
    }

    @Override
    public int get(final ByteBuf buff) {
      return buff.getInt(0);
    }

    @Override
    public ByteBuf set(final int val) {
      return nb(val);
    }

    @Override
    public void set(final ByteBuf buff, final int val) {
      buff.setInt(0, val);
    }

  }

  /**
   * Adapter to allow different buffers to be tested with this class.
   *
   * @param <T> buffer type
   */
  private interface BufferRunner<T> {

    void execute(TemporaryFolder tmp);

    T set(int val);

    void set(T buff, int val);

    int get(T buff);
  }

}
