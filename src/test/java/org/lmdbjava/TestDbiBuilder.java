package org.lmdbjava;

import static com.jakewharton.byteunits.BinaryByteUnit.MEBIBYTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.bb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestDbiBuilder {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();
    private Env<ByteBuffer> env;

    @After
    public void after() {
        env.close();
    }

    @Before
    public void before() throws IOException {
        System.out.println("before");
        final File path = tmp.newFile();
        env =
                create()
                        .setMapSize(MEBIBYTES.toBytes(64))
                        .setMaxReaders(2)
                        .setMaxDbs(2)
                        .open(path, MDB_NOSUBDIR);
    }

    @Test
    public void unnamed() {
        final Dbi<ByteBuffer> dbi = env.buildDbi()
                .withoutDbName()
                .withDefaultComparator()
                .withDbiFlags(DbiFlags.MDB_CREATE)
                .open();

        assertThat(env.getDbiNames().size(), Matchers.is(0));

        assertPutAndGet(dbi);
    }


    @Test
    public void named() {
        final Dbi<ByteBuffer> dbi = env.buildDbi()
                .withDbName("foo")
                .withDefaultComparator()
                .withDbiFlags(DbiFlags.MDB_CREATE)
                .open();

        assertPutAndGet(dbi);

        assertThat(env.getDbiNames().size(), Matchers.is(1));
        assertThat(new String(env.getDbiNames().get(0), StandardCharsets.UTF_8), Matchers.is("foo"));
    }

    private void assertPutAndGet(Dbi<ByteBuffer> dbi) {
        try (Txn<ByteBuffer> writeTxn = env.txnWrite()) {
            dbi.put(writeTxn, bb(123), bb(123_000));
            writeTxn.commit();
        }

        try (Txn<ByteBuffer> readTxn = env.txnRead()) {
            final ByteBuffer byteBuffer = dbi.get(readTxn, bb(123));
            final int val = byteBuffer.getInt();
            assertThat(val, Matchers.is(123_000));
        }
    }
}
