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

import org.junit.Test;
import org.lmdbjava.ByteBufferProxy.AbstractByteBufferProxy;
import org.lmdbjava.CursorIterable.KeyVal;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

/**
 * Test {@link CursorIterable}.
 */
public final class CursorIterableRangeTest {

    @Test
    public void testSignedComparator() throws IOException {
        test(ByteBuffer::compareTo, true, "testSignedComparator", 1, MDB_CREATE);
    }

    @Test
    public void testUnsignedComparator() throws IOException {
        test(AbstractByteBufferProxy::compareBuff, false, "testUnsignedComparator", 1, MDB_CREATE);
    }

    @Test
    public void testSignedComparatorDupsort() throws IOException {
        test(ByteBuffer::compareTo, true, "testSignedComparatorDupsort", 2,MDB_CREATE, MDB_DUPSORT);
    }

    @Test
    public void testUnsignedComparatorDupsort() throws IOException {
        test(AbstractByteBufferProxy::compareBuff, false, "testUnsignedComparatorDupsort",2, MDB_CREATE, MDB_DUPSORT);
    }

    private void test(final Comparator<ByteBuffer> comparator,
                      final boolean nativeCb,
                      final String testName,
                      final int copies,
                      final DbiFlags... flags) throws IOException {
        final Path dbPath = Files.createTempFile("test", "db");
        try (final Env<ByteBuffer> env =
                create()
                        .setMapSize(KIBIBYTES.toBytes(256))
                        .setMaxReaders(1)
                        .setMaxDbs(1)
                        .open(dbPath.toFile(), POSIX_MODE, MDB_NOSUBDIR)) {
            final Dbi<ByteBuffer> dbi = env.openDbi(DB_1, comparator, nativeCb, flags);
            populateDatabase(env, dbi, copies);

            final File tests = new File("src/test/resources/CursorIterableRangeTest/tests.csv");
            final File actual = tests.getParentFile().toPath().resolve(testName + ".actual").toFile();
            final File expected = tests.getParentFile().toPath().resolve(testName + ".expected").toFile();
            final String csv = readFile(tests);
            final String[] parts = csv.split("\n");
            try (final Writer writer = new FileWriter(actual)) {
                for (final String part : parts) {
                    writer.append(part);
                    writer.append(" =>");

                    final String[] params = part.split(",");
                    final KeyRangeType keyRangeType = KeyRangeType.valueOf(params[0].trim());
                    ByteBuffer start = null;
                    ByteBuffer stop = null;
                    if (params.length > 1 && params[1].trim().length() > 0) {
                        start = bb(Integer.parseInt(params[1].trim()));
                    }
                    if (params.length > 2 && params[2].trim().length() > 0) {
                        stop = bb(Integer.parseInt(params[2].trim()));
                    }
                    final KeyRange<ByteBuffer> keyRange = new KeyRange<>(keyRangeType, start, stop);
                    boolean first = true;
                    try (Txn<ByteBuffer> txn = env.txnRead();
                            CursorIterable<ByteBuffer> c = dbi.iterate(txn, keyRange)) {
                        for (final KeyVal<ByteBuffer> kv : c) {
                            if (first) {
                                first = false;
                            } else {
                                writer.append(",");
                            }

                            final int key = kv.key().getInt();
                            final int val = kv.val().getInt();
                            writer.append(" [");
                            writer.append(String.valueOf(key));
                            writer.append(",");
                            writer.append(String.valueOf(val));
                            writer.append("]");
                        }
                    }
                    writer.append("\n");
                }
            }

            // Compare files.
            final String act = readFile(actual);
            final String exp = readFile(expected);
            assertThat("Files are not equal", act.equals(exp));
        } finally {
            deleteFile(dbPath);
        }
    }

    private void populateDatabase(final Env<ByteBuffer> env,
                                  final Dbi<ByteBuffer> dbi,
                                  final int copies) {
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            final Cursor<ByteBuffer> c = dbi.openCursor(txn);
            for (int i = 0; i < copies; i++) {
                c.put(bb(0), bb(1 + i));
                c.put(bb(2), bb(3 + i));
                c.put(bb(4), bb(5 + i));
                c.put(bb(6), bb(7 + i));
                c.put(bb(8), bb(9 + i));
                c.put(bb(-2), bb(-1 + i));
            }
            txn.commit();
        }
    }

    private String readFile(final File file) throws IOException {
        final StringBuilder result = new StringBuilder();
        try (final Reader reader = new FileReader(file)) {
            final char[] cbuf = new char[4096];
            int nread;
            while ((nread = reader.read(cbuf, 0, cbuf.length)) != -1) {
                result.append(cbuf, 0, nread);
            }
        }
        return result.toString();
    }

    private boolean recursiveDeleteFiles(Path file) {
        if (deleteFile(file)) {
            return true;
        } else {
            try (final Stream<Path> stream = Files.list(file)) {
                stream.forEach(this::recursiveDeleteFiles);
            } catch (final IOException e) {
                return false;
            }
            return deleteFile(file);
        }
    }

    private boolean deleteFile(Path file) {
        try {
            Files.delete(file);
        } catch (final IOException e) {
            return false;
        }
        return true;
    }
}
