package org.lmdbjava;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

public final class Issue17 {

  @Test
  public void test() {
    final File path = new File("lmdbTest");
    path.deleteOnExit();

    Env<ByteBuffer> env = Env.create().setMapSize(MEBIBYTES.toBytes(8))
        .setMaxDbs(1).open(path, EnvFlags.MDB_NOSUBDIR);
    Dbi<ByteBuffer> db = env.openDbi("test", DbiFlags.MDB_CREATE);

    try {
      byte[] k = new byte[500];
      ByteBuffer key = ByteBuffer.allocateDirect(500);
      ByteBuffer val = ByteBuffer.allocateDirect(1024);

      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      int count = 0;
      for (;;) {
        rnd.nextBytes(k);
        key.clear();
        key.put(k).flip();
        val.clear();
        db.put(key, val);
        System.out.println("written " + ++count);
      }
    } catch (Env.MapFullException e) {
      // expected
    }

    System.out.println("closing db");
    db.close();

    System.out.println("closing env");
    env.close();
  }

}