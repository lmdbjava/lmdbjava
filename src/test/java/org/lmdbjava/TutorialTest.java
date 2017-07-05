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
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.concurrent.ExecutorService;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CursorIterator.IteratorType.BACKWARD;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import org.lmdbjava.CursorIterator.KeyVal;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;

/**
 * Welcome to LmdbJava!
 *
 * <p>
 * This short tutorial will walk you through using LmdbJava step-by-step.
 *
 * <p>
 * If you are using a 64-bit Windows, Linux or OS X machine, you can simply run
 * this tutorial by adding the LmdbJava JAR to your classpath. It includes the
 * required system libraries. If you are using another 64-bit platform, you'll
 * need to install the LMDB system library yourself. 32-bit platforms are not
 * supported.
 */
public final class TutorialTest {

  private static final String DB_NAME = "my DB";
  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  /**
   * In this first tutorial we will use LmdbJava with some basic defaults.
   *
   * @throws IOException if a path was unavailable for memory mapping
   */
  @Test
  public void tutorial1() throws IOException {
    // We need a storage directory first.
    // The path cannot be on a remote file system.
    final File path = tmp.newFolder();

    // We always need an Env. An Env owns a physical on-disk storage file. One
    // Env can store many different databases (ie sorted maps).
    final Env<ByteBuffer> env = create()
        // LMDB also needs to know how large our DB might be. Over-estimating is OK.
        .setMapSize(10_485_760)
        // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
        .setMaxDbs(1)
        // Now let's open the Env. The same path can be concurrently opened and
        // used in different processes, but do not open the same path twice in
        // the same process at the same time.
        .open(path);

    // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
    // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
    final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

    // We want to store some data, so we will need a direct ByteBuffer.
    // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
    // Values can be larger.
    final ByteBuffer key = allocateDirect(env.getMaxKeySize());
    final ByteBuffer val = allocateDirect(700);
    key.put("greeting".getBytes(UTF_8)).flip();
    val.put("Hello world".getBytes(UTF_8)).flip();
    final int valSize = val.remaining();

    // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
    db.put(key, val);

    // To fetch any data from LMDB we need a Txn. A Txn is very important in
    // LmdbJava because it offers ACID characteristics and internally holds a
    // read-only key buffer and read-only value buffer. These read-only buffers
    // are always the same two Java objects, but point to different LMDB-managed
    // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
    // valid only until the Txn is released or the next Dbi or Cursor call. If
    // you need data afterwards, you should copy the bytes to your own buffer.
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer found = db.get(txn, key);
      assertNotNull(found);

      // The fetchedVal is read-only and points to LMDB memory
      final ByteBuffer fetchedVal = txn.val();
      assertThat(fetchedVal.remaining(), is(valSize));

      // Let's double-check the fetched value is correct
      assertThat(UTF_8.decode(fetchedVal).toString(), is("Hello world"));
    }

    // We can also delete. The simplest way is to let Dbi allocate a new Txn...
    db.delete(key);

    // Now if we try to fetch the deleted row, it won't be present
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      assertNull(db.get(txn, key));
    }
  }

  /**
   * In this second tutorial we'll learn more about LMDB's ACID Txns.
   *
   * @throws IOException          if a path was unavailable for memory mapping
   * @throws InterruptedException if executor shutdown interrupted
   */
  @Test
  @SuppressWarnings({"ConvertToTryWithResources",
                     "checkstyle:executablestatementcount"})
  public void tutorial2() throws IOException, InterruptedException {
    final File path = tmp.newFolder();
    // Here we use a shortcut to open a 10 MB environment with one database.
    final Env<ByteBuffer> env = open(path, 10);

    final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);
    final ByteBuffer key = allocateDirect(env.getMaxKeySize());
    final ByteBuffer val = allocateDirect(700);

    // Let's write and commit "key1" via a Txn. A Txn can include multiple Dbis.
    // Note write Txns block other write Txns, due to writes being serialized.
    // It's therefore important to avoid unnecessarily long-lived write Txns.
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      key.put("key1".getBytes(UTF_8)).flip();
      val.put("lmdb".getBytes(UTF_8)).flip();
      db.put(txn, key, val);

      // We can read data too, even though this is a write Txn.
      final ByteBuffer found = db.get(txn, key);
      assertNotNull(found);

      // An explicit commit is required, otherwise Txn.close() rolls it back.
      txn.commit();
    }

    // Open a read-only Txn. It only sees data that existed at Txn creation time.
    final Txn<ByteBuffer> rtx = env.txnRead();

    // Our read Txn can fetch key1 without problem, as it existed at Txn creation.
    ByteBuffer found = db.get(rtx, key);
    assertNotNull(found);

    // Note that our main test thread holds the Txn. Only one Txn per thread is
    // typically permitted (the exception is a read-only Env with MDB_NOTLS).
    //
    // Let's write out a "key2" via a new write Txn in a different thread.
    final ExecutorService es = newCachedThreadPool();
    es.execute(() -> {
      try (Txn<ByteBuffer> txn = env.txnWrite()) {
        key.clear();
        key.put("key2".getBytes(UTF_8)).flip();
        db.put(txn, key, val);
        txn.commit();
      }
    });
    es.shutdown();
    es.awaitTermination(10, SECONDS);

    // Even though key2 has been committed, our read Txn still can't see it.
    found = db.get(rtx, key);
    assertNull(found);

    // To see key2, we could create a new Txn. But a reset/renew is much faster.
    // Reset/renew is also important to avoid long-lived read Txns, as these
    // prevent the re-use of free pages by write Txns (ie the DB will grow).
    rtx.reset();
    // ... potentially long operation here ...
    rtx.renew();
    found = db.get(rtx, key);
    assertNotNull(found);

    // Don't forget to close the read Txn now we're completely finished. We could
    // have avoided this if we used a try-with-resources block, but we wanted to
    // play around with multiple concurrent Txns to demonstrate the "I" in ACID.
    rtx.close();
  }

  /**
   * In this third tutorial we'll have a look at the Cursor. Up until now we've
   * just used Dbi, which is good enough for simple cases but unsuitable if you
   * don't know the key to fetch, or want to iterate over all the data etc.
   *
   * @throws IOException if a path was unavailable for memory mapping
   */
  @Test
  @SuppressWarnings({"ConvertToTryWithResources",
                     "checkstyle:executablestatementcount"})
  public void tutorial3() throws IOException {
    // As per tutorial1...
    final File path = tmp.newFolder();
    final Env<ByteBuffer> env = open(path, 10);
    final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);
    final ByteBuffer key = allocateDirect(env.getMaxKeySize());
    final ByteBuffer val = allocateDirect(700);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // A cursor always belongs to a particular Dbi.
      final Cursor<ByteBuffer> c = db.openCursor(txn);

      // We can put via a Cursor. Note we're adding keys in a strange order,
      // as we want to show you that LMDB returns them in sorted order.
      key.put("zzz".getBytes(UTF_8)).flip();
      val.put("lmdb".getBytes(UTF_8)).flip();
      c.put(key, val);
      key.clear();
      key.put("aaa".getBytes(UTF_8)).flip();
      c.put(key, val);
      key.clear();
      key.put("ccc".getBytes(UTF_8)).flip();
      c.put(key, val);

      // We can read from the Cursor by key.
      c.get(key, MDB_SET);
      assertThat(UTF_8.decode(c.key()).toString(), is("ccc"));

      // Let's see that LMDB provides the keys in appropriate order....
      c.seek(MDB_FIRST);
      assertThat(UTF_8.decode(c.key()).toString(), is("aaa"));

      c.seek(MDB_LAST);
      assertThat(UTF_8.decode(c.key()).toString(), is("zzz"));

      c.seek(MDB_PREV);
      assertThat(UTF_8.decode(c.key()).toString(), is("ccc"));

      // Cursors can also delete the current key.
      c.delete();

      c.close();
      txn.commit();
    }

    // A read-only Cursor can survive its original Txn being closed. This is
    // useful if you want to close the original Txn (eg maybe you created the
    // Cursor during the constructor of a singleton with a throw-away Txn). Of
    // course, you cannot use the Cursor if its Txn is closed or currently reset.
    final Txn<ByteBuffer> tx1 = env.txnRead();
    final Cursor<ByteBuffer> c = db.openCursor(tx1);
    tx1.close();

    // The Cursor becomes usable again by "renewing" it with an active read Txn.
    final Txn<ByteBuffer> tx2 = env.txnRead();
    c.renew(tx2);
    c.seek(MDB_FIRST);

    // As usual with read Txns, we can reset and renew them. The Cursor does
    // not need any special handling if we do this.
    tx2.reset();
    // ... potentially long operation here ...
    tx2.renew();
    c.seek(MDB_LAST);

    tx2.close();
  }

  /**
   * In this fourth tutorial we'll take a quick look at the iterators. These are
   * a more Java idiomatic form of using the Cursors we looked at in tutorial 3.
   *
   * @throws IOException if a path was unavailable for memory mapping
   */
  @Test
  public void tutorial4() throws IOException {
    // As per tutorial1...
    final File path = tmp.newFolder();
    final Env<ByteBuffer> env = open(path, 10);
    final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer key = allocateDirect(env.getMaxKeySize());
      final ByteBuffer val = allocateDirect(700);

      // Insert some data. Note the ByteBuffer byte order is platform specific.
      // LMDB does not store or set byte order, but it's critical to sort keys.
      // If your numeric keys don't sort as expected, review buffer byte order.
      val.putInt(100);
      key.putInt(1);
      db.put(txn, key, val);
      key.clear();
      key.putInt(2);
      db.put(txn, key, val);
      key.clear();

      // Each iterator uses a cursor and must be closed when finished.
      // iterate forward in terms of key ordering starting with the first key
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, FORWARD)) {
        for (final KeyVal<ByteBuffer> kv : it.iterable()) {
          assertThat(kv.key(), notNullValue());
          assertThat(kv.val(), notNullValue());
        }
      }

      // iterate backward in terms of key ordering starting with the first key
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, BACKWARD)) {
        for (final KeyVal<ByteBuffer> kv : it.iterable()) {
          assertThat(kv.key(), notNullValue());
          assertThat(kv.val(), notNullValue());
        }
      }

      // search for key and iterate forwards/backward from there til the last/first key.
      key.putInt(1);
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, key, FORWARD)) {
        for (final KeyVal<ByteBuffer> kv : it.iterable()) {
          assertThat(kv.key(), notNullValue());
          assertThat(kv.val(), notNullValue());
        }
      }
    }
  }

  /**
   * In this fifth tutorial we'll explore multiple values sharing a single key.
   *
   * @throws IOException if a path was unavailable for memory mapping
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial5() throws IOException {
    // As per tutorial1...
    final File path = tmp.newFolder();
    final Env<ByteBuffer> env = open(path, 10);

    // This time we're going to tell the Dbi it can store > 1 value per key.
    // There are other flags available if we're storing integers etc.
    final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE, MDB_DUPSORT);

    // Duplicate support requires both keys and values to be <= max key size.
    final ByteBuffer key = allocateDirect(env.getMaxKeySize());
    final ByteBuffer val = allocateDirect(env.getMaxKeySize());

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = db.openCursor(txn);

      // Store one key, but many values, and in non-natural order.
      key.put("key".getBytes(UTF_8)).flip();
      val.put("xxx".getBytes(UTF_8)).flip();
      c.put(key, val);
      val.clear();
      val.put("kkk".getBytes(UTF_8)).flip();
      c.put(key, val);
      val.clear();
      val.put("lll".getBytes(UTF_8)).flip();
      c.put(key, val);

      // Cursor can tell us how many values the current key has.
      final long count = c.count();
      assertThat(count, is(3L));

      // Let's position the Cursor. Note sorting still works.
      c.seek(MDB_FIRST);
      assertThat(UTF_8.decode(c.val()).toString(), is("kkk"));

      c.seek(MDB_LAST);
      assertThat(UTF_8.decode(c.val()).toString(), is("xxx"));

      c.seek(MDB_PREV);
      assertThat(UTF_8.decode(c.val()).toString(), is("lll"));

      c.close();
      txn.commit();
    }
  }

  /**
   * In this final tutorial we'll look at using Agrona's DirectBuffer.
   *
   * @throws IOException if a path was unavailable for memory mapping
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial6() throws IOException {
    // The critical difference is we pass the PROXY_DB field to Env.create().
    // There's also a PROXY_SAFE if you want to stop ByteBuffer's Unsafe use.
    // Aside from that and a different type argument, it's the same as usual...
    final File path = tmp.newFolder();
    final Env<DirectBuffer> env = create(PROXY_DB)
        .setMapSize(10_485_760)
        .setMaxDbs(1)
        .open(path);

    final Dbi<DirectBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

    final ByteBuffer keyBb = allocateDirect(env.getMaxKeySize());
    final MutableDirectBuffer key = new UnsafeBuffer(keyBb);
    final MutableDirectBuffer val = new UnsafeBuffer(allocateDirect(700));

    try (Txn<DirectBuffer> txn = env.txnWrite()) {
      final Cursor<DirectBuffer> c = db.openCursor(txn);

      // Agrona is not only faster than ByteBuffer, but its methods are nicer...
      val.putStringWithoutLengthUtf8(0, "The Value");
      key.putStringWithoutLengthUtf8(0, "yyy");
      c.put(key, val);

      key.putStringWithoutLengthUtf8(0, "ggg");
      c.put(key, val);

      c.seek(MDB_FIRST);
      assertThat(c.key().getStringWithoutLengthUtf8(0, env.getMaxKeySize()),
                 startsWith("ggg"));

      c.seek(MDB_LAST);
      assertThat(c.key().getStringWithoutLengthUtf8(0, env.getMaxKeySize()),
                 startsWith("yyy"));

      // DirectBuffer has no notion of a position. Often you don't want to store
      // the unnecessary bytes of a varying-size buffer. Let's have a look...
      final int keyLen = key.putStringWithoutLengthUtf8(0, "12characters");
      assertThat(keyLen, is(12));
      assertThat(key.capacity(), is(env.getMaxKeySize()));

      // To only store the 12 characters, we simply call wrap:
      key.wrap(key, 0, keyLen);
      assertThat(key.capacity(), is(keyLen));
      c.put(key, val);
      c.seek(MDB_FIRST);
      assertThat(c.key().capacity(), is(keyLen));
      assertThat(c.key().getStringWithoutLengthUtf8(0, c.key().capacity()),
                 is("12characters"));

      // If we want to store bigger values again, just wrap our original buffer.
      key.wrap(keyBb);
      assertThat(key.capacity(), is(env.getMaxKeySize()));

      c.close();
      txn.commit();
    }
  }

  // You've finished! There are lots of other neat things we could show you (eg
  // how to speed up inserts by appending them in key order, using integer
  // or reverse ordered keys, using Env.DISABLE_CHECKS_PROP etc), but you now
  // know enough to tackle the JavaDocs with confidence. Have fun!
}
