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

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static java.lang.Long.BYTES;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.lmdbjava.ByteArrayProxy.PROXY_BA;
import static org.lmdbjava.ByteBufProxy.PROXY_NETTY;
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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

/** Test {@link Cursor} with different buffer implementations. */
public final class CursorParamTest {

  static class MyArgumentProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations parameters, ExtensionContext context) {
      return Stream.of(
          Arguments.argumentSet(
              "ByteBufferRunner(PROXY_OPTIMAL)", new ByteBufferRunner(PROXY_OPTIMAL)),
          Arguments.argumentSet("ByteBufferRunner(PROXY_SAFE)", new ByteBufferRunner(PROXY_SAFE)),
          Arguments.argumentSet("ByteArrayRunner(PROXY_BA)", new ByteArrayRunner(PROXY_BA)),
          Arguments.argumentSet("DirectBufferRunner", new DirectBufferRunner()),
          Arguments.argumentSet("NettyBufferRunner", new NettyBufferRunner()));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(MyArgumentProvider.class)
  void execute(final BufferRunner<?> runner, @TempDir final Path tmp) {
    runner.execute(tmp);
  }

  /**
   * Abstract implementation of {@link BufferRunner}.
   *
   * @param <T> buffer type
   */
  private abstract static class AbstractBufferRunner<T> implements BufferRunner<T> {

    final BufferProxy<T> proxy;

    protected AbstractBufferRunner(final BufferProxy<T> proxy) {
      this.proxy = proxy;
    }

    @Override
    public final void execute(final Path tmp) {
      try (Env<T> env = env(tmp)) {
        assertThat(env.getDbiNames()).isEmpty();
        final Dbi<T> db = env.openDbi(DB_1, MDB_CREATE, MDB_DUPSORT);
        assertThat(env.getDbiNames().get(0)).isEqualTo(DB_1.getBytes(UTF_8));
        try (Txn<T> txn = env.txnWrite();
            Cursor<T> c = db.openCursor(txn)) {
          // populate data
          c.put(set(1), set(2), MDB_NOOVERWRITE);
          c.put(set(3), set(4));
          c.put(set(5), set(6));
          // we cannot set the value for ByteArrayProxy
          // but the key is still valid.
          final T valForKey7 = c.reserve(set(7), BYTES);
          set(valForKey7, 8);

          // check MDB_SET operations
          final T key3 = set(3);
          assertThat(c.get(key3, MDB_SET_KEY)).isTrue();
          assertThat(get(c.key())).isEqualTo(3);
          assertThat(get(c.val())).isEqualTo(4);
          final T key6 = set(6);
          assertThat(c.get(key6, MDB_SET_RANGE)).isTrue();
          assertThat(get(c.key())).isEqualTo(7);
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(get(c.val())).isEqualTo(8);
          }
          final T key999 = set(999);
          assertThat(c.get(key999, MDB_SET_KEY)).isFalse();

          // check MDB navigation operations
          assertThat(c.seek(MDB_LAST)).isTrue();
          final int mdb1 = get(c.key());
          final int mdb2 = get(c.val());

          assertThat(c.seek(MDB_PREV)).isTrue();
          final int mdb3 = get(c.key());
          final int mdb4 = get(c.val());

          assertThat(c.seek(MDB_NEXT)).isTrue();
          final int mdb5 = get(c.key());
          final int mdb6 = get(c.val());

          assertThat(c.seek(MDB_FIRST)).isTrue();
          final int mdb7 = get(c.key());
          final int mdb8 = get(c.val());

          // assert afterwards to ensure memory address from LMDB
          // are valid within same txn and across cursor movement
          // MDB_LAST
          assertThat(mdb1).isEqualTo(7);
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(mdb2).isEqualTo(8);
          }

          // MDB_PREV
          assertThat(mdb3).isEqualTo(5);
          assertThat(mdb4).isEqualTo(6);

          // MDB_NEXT
          assertThat(mdb5).isEqualTo(7);
          if (!(this instanceof ByteArrayRunner)) {
            assertThat(mdb6).isEqualTo(8);
          }

          // MDB_FIRST
          assertThat(mdb7).isEqualTo(1);
          assertThat(mdb8).isEqualTo(2);
        }
      }
    }

    private Env<T> env(final Path tmp) {
      return create(proxy)
          .setMapSize(MEBIBYTES.toBytes(1))
          .setMaxReaders(1)
          .setMaxDbs(1)
          .setFilePermissions(POSIX_MODE)
          .setEnvFlags(MDB_NOSUBDIR)
          .open(tmp.resolve("db"));
    }
  }

  /** {@link BufferRunner} for Java byte buffers. */
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

  /** {@link BufferRunner} for Java byte buffers. */
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

  /** {@link BufferRunner} for Agrona direct buffer. */
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

  /** {@link BufferRunner} for Netty byte buf. */
  private static class NettyBufferRunner extends AbstractBufferRunner<ByteBuf> {

    NettyBufferRunner() {
      super(PROXY_NETTY);
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

    void execute(Path tmp);

    T set(int val);

    void set(T buff, int val);

    int get(T buff);
  }
}
