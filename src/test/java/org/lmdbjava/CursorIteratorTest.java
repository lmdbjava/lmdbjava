package org.lmdbjava;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterator.IteratorType;
import org.lmdbjava.CursorIterator.KeyVal;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.*;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.createBb;

public class CursorIteratorTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  Env<ByteBuffer> env;
  Dbi<ByteBuffer> db;
  LinkedList<Integer> list = new LinkedList<>();

  private Env<ByteBuffer> makeEnv() {
    try {
      final File path = tmp.newFile();
      env = create(PROXY_OPTIMAL)
        .setMapSize(1_024 * 1_024)
        .setMaxDbs(1)
        .setMaxReaders(1)
        .open(path, POSIX_MODE, MDB_NOSUBDIR);
      db = env.openDbi(DB_1, MDB_CREATE);
      list.addAll(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
      try (final Txn<ByteBuffer> txn = env.txnWrite()) {
        final Cursor<ByteBuffer> c = db.openCursor(txn);
        c.put(createBb(1), createBb(2), MDB_NOOVERWRITE);
        c.put(createBb(3), createBb(4));
        c.put(createBb(5), createBb(6));
        c.put(createBb(7), createBb(8));
        txn.commit();
      }
      return env;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void iterate() {
    final Env<ByteBuffer> env = makeEnv();

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterator<ByteBuffer> c = db.iterate(txn)) {
        for (KeyVal<ByteBuffer> kv : c.iterable()) {
          assertThat(kv.key.getInt(), is(list.pollFirst()));
          assertThat(kv.val.getInt(), is(list.pollFirst()));
        }
      }
    }
  }

  @Test
  public void forward() {
    final Env<ByteBuffer> env = makeEnv();

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterator<ByteBuffer> c = db.iterate(txn, IteratorType.FORWARD)) {
        for (KeyVal<ByteBuffer> kv : c.iterable()) {
          assertThat(kv.key.getInt(), is(list.pollFirst()));
          assertThat(kv.val.getInt(), is(list.pollFirst()));
        }
      }
    }
  }

  @Test
  public void forwardSeek() {
    final Env<ByteBuffer> env = makeEnv();

    ByteBuffer key = createBb(3);
    list.pollFirst();
    list.pollFirst();

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterator<ByteBuffer> c = db.iterate(txn, key, IteratorType.FORWARD)) {
        for (KeyVal<ByteBuffer> kv : c.iterable()) {
          assertThat(kv.key.getInt(), is(list.pollFirst()));
          assertThat(kv.val.getInt(), is(list.pollFirst()));
        }
      }
    }
  }


  @Test
  public void backward() {
    final Env<ByteBuffer> env = makeEnv();

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterator<ByteBuffer> c = db.iterate(txn, IteratorType.BACKWARD)) {
        for (KeyVal<ByteBuffer> kv : c.iterable()) {
          assertThat(kv.val.getInt(), is(list.pollLast()));
          assertThat(kv.key.getInt(), is(list.pollLast()));
        }
      }
    }
  }

  @Test
  public void backwardSeek() {
    final Env<ByteBuffer> env = makeEnv();
    ByteBuffer key = createBb(5);
    list.pollLast();
    list.pollLast();
    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterator<ByteBuffer> c = db.iterate(txn, key, IteratorType.BACKWARD)) {
        for (KeyVal<ByteBuffer> kv : c.iterable()) {
          assertThat(kv.val.getInt(), is(list.pollLast()));
          assertThat(kv.key.getInt(), is(list.pollLast()));
        }
      }
    }
  }
}
