/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterator.KeyVal;

import static org.lmdbjava.ByteUnit.MEBIBYTES;
import static org.lmdbjava.CursorIterator.IteratorType.BACKWARD;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_MDB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.Env.open;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;

/**
 * Welcome to LmdbJava!
 * <p>
 * This short tutorial will walk you through using LmdbJava step-by-step.
 * <p>
 * To complete this tutorial you will need to have installed the LMDB shared
 * library on your system. It isn't packaged with LmdbJava, so use your
 * operating system's package manager to install it.
 */
public class TutorialTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  /**
   * In this first tutorial we will use LmdbJava with some basic defaults.
   *
   * @throws java.io.IOException
   */
  @Test
  public void tutorial1() throws IOException {
    // We need a storage directory first.
    // The path cannot be on a remote file system.
    File path = tmp.newFolder();

    // We always need an Env. An Env owns a physical on-disk storage file. One
    // Env can store many different databases (ie sorted maps).
    Env<ByteBuffer> env = create()
        // LMDB also needs to know how large our DB might be. Over-estimating is OK.
        .setMapSize(10, MEBIBYTES)
        // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
        .setMaxDbs(1)
        // Now let's open the Env. The 0664 is the POSIX mode of the created files.
        // The same path can be concurrently opened and used in different processes,
        // but do not open the same path twice in the same process at the same time.
        .open(path, 0664);

    // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
    // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
    Dbi<ByteBuffer> db = env.openDbi("my DB", MDB_CREATE);

    // We want to store some data, so we will need a direct ByteBuffer.
    // Note that LMDB keys cannot exceed 511 bytes. Values can be larger.
    ByteBuffer key = allocateDirect(511);
    ByteBuffer val = allocateDirect(700);
    key.put("greeting".getBytes());
    val.put("Hello world".getBytes());

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
      ByteBuffer found = db.get(txn, key);
      assertNotNull(found);

      // The fetchedVal is read-only and points to LMDB memory
      ByteBuffer fetchedVal = txn.val();
      assertThat(fetchedVal.capacity(), is(700));
      assertThat(fetchedVal.limit(), is(700));

      // Let's double-check the fetched value is correct
      byte[] fetched = new byte[700];
      fetchedVal.get(fetched);
      assertThat(new String(fetched), startsWith("Hello world"));
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
   * @throws java.io.IOException
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial2() throws IOException {
    // As per tutorial1...
    File path = tmp.newFolder();
    // open 10 mb environment with one database.
    Env<ByteBuffer> env = open(path, 10);
    Dbi<ByteBuffer> db = env.openDbi("my DB", MDB_CREATE);
    ByteBuffer key = allocateDirect(511);
    ByteBuffer val = allocateDirect(700);

    // Let's write and commit "key1" via a Txn. A Txn can include multiple Dbis.
    // Note write Txns block other write Txns, due to writes being serialized.
    // It's therefore important to avoid unnecessarily long-lived write Txns.
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      key.put("key1".getBytes());
      val.put("lmdb".getBytes());
      db.put(txn, key, val);

      // We can read data too, even though this is a write Txn.
      ByteBuffer found = db.get(txn, key);
      assertNotNull(found);

      // An explicit commit is required, otherwise Txn.close() rolls it back.
      txn.commit();
    }

    // Open a read-only Txn. It only sees data that existed at Txn creation time.
    Txn<ByteBuffer> rtx = env.txnRead();

    // Our read Txn can fetch key1 without problem, as it existed at Txn creation.
    ByteBuffer found = db.get(rtx, key);
    assertNotNull(found);

    // Let's write out a "key2" via a new write Txn.
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      key.clear();
      key.put("key2".getBytes());
      db.put(txn, key, val);
      txn.commit();
    }

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
   * @throws java.io.IOException
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial3() throws IOException {
    // As per tutorial1...
    File path = tmp.newFolder();
    Env<ByteBuffer> env = create()
        .setMapSize(10, MEBIBYTES)
        .setMaxDbs(1)
        .open(path, 0664);
    Dbi<ByteBuffer> db = env.openDbi("my DB", MDB_CREATE);
    ByteBuffer key = allocateDirect(511);
    ByteBuffer val = allocateDirect(700);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      // A cursor always belongs to a particular Dbi.
      Cursor<ByteBuffer> c = db.openCursor(txn);

      // We can put via a Cursor. Note we're adding keys in a strange order,
      // as we want to show you that LMDB returns them in sorted order.
      key.put("zzz".getBytes());
      val.put("lmdb".getBytes());
      c.put(key, val);
      key.clear();
      key.put("aaa".getBytes());
      c.put(key, val);
      key.clear();
      key.put("ccc".getBytes());
      c.put(key, val);

      // Just a byte array to hold our keys (ByteBuffer isn't fun to use!)...
      byte[] keyBytes = new byte[511];

      // We can read from the Cursor by key.
      c.get(key, MDB_SET);
      txn.key().get(keyBytes);
      assertThat(new String(keyBytes), startsWith("ccc"));

      // Let's see that LMDB provides the keys in appropriate order....
      c.seek(MDB_FIRST);
      txn.key().get(keyBytes);
      assertThat(new String(keyBytes), startsWith("aaa"));

      c.seek(MDB_LAST);
      txn.key().get(keyBytes);
      assertThat(new String(keyBytes), startsWith("zzz"));

      c.seek(MDB_PREV);
      txn.key().get(keyBytes);
      assertThat(new String(keyBytes), startsWith("ccc"));

      // Cursors can also delete the current key.
      c.delete();

      c.close();
      txn.commit();
    }

    // Iterators are provided as a convenience. Each iterator
    // uses a cursor and must be closed when finished.
    try (Txn<ByteBuffer> txn = env.txnWrite()) {

      // iterate forward in terms of key ordering starting with the first key
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, FORWARD)) {
        for (KeyVal<ByteBuffer> kv : it.iterable()) {
          ByteBuffer k = kv.key;
          ByteBuffer v = kv.val;
        }
      }

      // iterate backward in terms of key ordering starting with the first key
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, BACKWARD)) {
        for (KeyVal<ByteBuffer> kv : it.iterable()) {
          ByteBuffer k = kv.key;
          ByteBuffer v = kv.val;
        }
      }

      // search for key and iterate forwards/backward from there til the last/first key.
      ByteBuffer searchKey = allocateDirect(511);
      searchKey.putLong(100L);
      try (CursorIterator<ByteBuffer> it = db.iterate(txn, searchKey, FORWARD)) {
        for (KeyVal<ByteBuffer> kv : it.iterable()) {
          ByteBuffer k = kv.key;
          ByteBuffer v = kv.val;
        }
      }
    }

    // A read-only Cursor can survive its original Txn being closed. This is
    // useful if you want to close the original Txn (eg maybe you created the
    // Cursor during the constructor of a singleton with a throw-away Txn). Of
    // course, you cannot use the Cursor if its Txn is closed or currently reset.
    Txn<ByteBuffer> tx1 = env.txnRead();
    Cursor<ByteBuffer> c = db.openCursor(tx1);
    tx1.close();

    // The Cursor becomes usable again by "renewing" it with an active read Txn.
    Txn<ByteBuffer> tx2 = env.txnRead();
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
   * In this fourth tutorial we'll explore multiple values sharing a single key.
   *
   * @throws java.io.IOException
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial4() throws IOException {
    // As per tutorial1...
    File path = tmp.newFolder();
    Env<ByteBuffer> env = create()
        .setMapSize(10, MEBIBYTES)
        .setMaxDbs(1)
        .open(path, 0664);

    // This time we're going to tell the Dbi it can store > 1 value per key.
    // There are other flags available if we're storing integers etc.
    Dbi<ByteBuffer> db = env.openDbi("my DB", MDB_CREATE, MDB_DUPSORT);

    // Duplicate support requires both keys and values to be <= 511 bytes.
    ByteBuffer key = allocateDirect(511);
    ByteBuffer val = allocateDirect(511);

    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      Cursor<ByteBuffer> c = db.openCursor(txn);

      // Store one key, but many values, and in non-natural order.
      key.put("key".getBytes());
      val.put("xxx".getBytes());
      c.put(key, val);
      val.clear();
      val.put("aaa".getBytes());
      c.put(key, val);
      val.clear();
      val.put("ccc".getBytes());
      c.put(key, val);

      // Cursor can tell us how many values the current key has.
      long count = c.count();
      assertThat(count, is(3L));

      byte[] valBytes = new byte[511];

      // Let's position the Cursor. Note sorting still works.
      c.seek(MDB_FIRST);
      txn.val().get(valBytes);
      assertThat(new String(valBytes), startsWith("aaa"));

      c.seek(MDB_LAST);
      txn.val().get(valBytes);
      assertThat(new String(valBytes), startsWith("xxx"));

      c.seek(MDB_PREV);
      txn.val().get(valBytes);
      assertThat(new String(valBytes), startsWith("ccc"));

      c.close();
      txn.commit();
    }
  }

  /**
   * In this final tutorial we'll look at using Agrona's MutableDirectBuffer.
   *
   * @throws java.io.IOException
   */
  @Test
  @SuppressWarnings("ConvertToTryWithResources")
  public void tutorial5() throws IOException {
    // The critical difference is we pass the PROXY_MDB field to Env.create().
    // There's also a PROXY_SAFE if you want to stop ByteBuffer's Unsafe use.
    // Aside from that and a different type argument, it's the same as usual...
    File path = tmp.newFolder();
    Env<DirectBuffer> env = create(PROXY_MDB)
        .setMapSize(10, MEBIBYTES)
        .setMaxDbs(1)
        .open(path, 0664);

    Dbi<DirectBuffer> db = env.openDbi("my DB", MDB_CREATE);

    MutableDirectBuffer key = new UnsafeBuffer(allocateDirect(511));
    MutableDirectBuffer val = new UnsafeBuffer(allocateDirect(700));

    try (Txn<DirectBuffer> txn = env.txnWrite()) {
      Cursor<DirectBuffer> c = db.openCursor(txn);

      // Agrona is not only faster than ByteBuffer, but its methods are nicer...
      val.putStringWithoutLengthUtf8(0, "The Value");
      key.putStringWithoutLengthUtf8(0, "xxx");
      c.put(key, val);

      key.putStringWithoutLengthUtf8(0, "aaa");
      c.put(key, val);

      c.seek(MDB_FIRST);
      assertThat(txn.key().getStringWithoutLengthUtf8(0, 511), startsWith("aaa"));

      c.seek(MDB_LAST);
      assertThat(txn.key().getStringWithoutLengthUtf8(0, 511), startsWith("xxx"));

      c.close();
      txn.commit();
    }
  }

  // You've finished! There are lots of other neat things we could show you (eg
  // how to speed up inserts by appending them in key order, using integer
  // or reverse ordered keys, using Env.DISABLE_CHECKS_PROP etc), but you now
  // know enough to tackle the JavaDocs with confidence. Have fun!
}
