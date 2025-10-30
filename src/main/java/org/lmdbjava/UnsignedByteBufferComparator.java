package org.lmdbjava;

import java.nio.ByteBuffer;
import java.util.Comparator;

class UnsignedByteBufferComparator implements Comparator<ByteBuffer> {

    @Override
    public int compare(final ByteBuffer o1, final ByteBuffer o2) {
        // Find the first index where the two buffers don't match.
        final int i = o1.mismatch(o2);

        // If the length of both buffers are equal and mismatch is the length then return 0 for equal.
        final int thisPos = o1.position();
        final int thisRem = o1.limit() - thisPos;
        final int thatPos = o2.position();
        final int thatRem = o2.limit() - thatPos;
        if (thisRem == thatRem && i == thatRem) {
            return 0;
        }

        if (i >= 0 && i < thisRem && i < thatRem) {
            return Byte.compareUnsigned(o1.get(thisPos + i), o2.get(thatPos + i));
        }

        return thisRem - thatRem;
    }
}
