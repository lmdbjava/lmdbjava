package org.lmdbjava;

import java.io.File;
import org.agrona.MutableDirectBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.lmdbjava.CursorOp.*;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.MutableDirectBufferProxy.MDB_FACTORY;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.allocateMdb;

public class MutableDirectBufferTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Env env;

  @Before
  public void before() throws Exception {
    env = create(MDB_FACTORY);
    final File path = tmp.newFile();
    env.setMapSize(1_024 * 1_024);
    env.setMaxDbs(1);
    env.setMaxReaders(1);
    env.open(path, POSIX_MODE, MDB_NOSUBDIR);
  }

  @Test
  public void getWithMutableByteBuffer() throws Exception {
    final Dbi<MutableDirectBuffer> db = env.openDbi(DB_1,
                                                    MDB_CREATE, MDB_DUPSORT);
    try (final Txn tx = env.txnWrite()) {
      // populate data
      final Cursor<MutableDirectBuffer> c = db.openCursor(tx);
      c.put(allocateMdb(db, 1), allocateMdb(db, 2), MDB_NOOVERWRITE);
      c.put(allocateMdb(db, 3), allocateMdb(db, 4));
      c.put(allocateMdb(db, 5), allocateMdb(db, 6));
      c.put(allocateMdb(db, 7), allocateMdb(db, 8));

      // check MDB_SET operations
      final MutableDirectBuffer key3 = allocateMdb(db, 3);
      assertThat(c.get(key3, MDB_SET_KEY), is(true));
      assertThat(c.key().getInt(0), is(3));
      assertThat(c.val().getInt(0), is(4));
      final MutableDirectBuffer key6 = allocateMdb(db, 6);
      assertThat(c.get(key6, MDB_SET_RANGE), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));

      // check MDB navigation operations
      assertThat(c.get(null, MDB_LAST), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_PREV), is(true));
      assertThat(c.key().getInt(0), is(5));
      assertThat(c.val().getInt(0), is(6));
      assertThat(c.get(null, MDB_NEXT), is(true));
      assertThat(c.key().getInt(0), is(7));
      assertThat(c.val().getInt(0), is(8));
      assertThat(c.get(null, MDB_FIRST), is(true));
      assertThat(c.key().getInt(0), is(1));
      assertThat(c.val().getInt(0), is(2));
    }
  }

}
