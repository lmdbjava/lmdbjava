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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;

public interface PutFlagSet extends FlagSet<PutFlags> {

    PutFlagSet EMPTY = PutFlagSetImpl.EMPTY;

    static PutFlagSet empty() {
        return PutFlagSetImpl.EMPTY;
    }

    static PutFlagSet of(final PutFlags putFlag) {
        Objects.requireNonNull(putFlag);
        return putFlag;
    }

    static PutFlagSet of(final PutFlags... putFlags) {
        return builder()
                .setFlags(putFlags)
                .build();
    }

    static PutFlagSet of(final Collection<PutFlags> putFlags) {
        return builder()
            .setFlags(putFlags)
            .build();
    }

    static AbstractFlagSet.Builder<PutFlags, PutFlagSet> builder() {
        return new AbstractFlagSet.Builder<>(
            PutFlags.class,
            PutFlagSetImpl::new,
            putFlag -> putFlag,
            EmptyPutFlagSet::new);
    }


    // --------------------------------------------------------------------------------


    class PutFlagSetImpl extends AbstractFlagSet<PutFlags> implements PutFlagSet {

        public static final PutFlagSet EMPTY = new EmptyPutFlagSet();

        private PutFlagSetImpl(final EnumSet<PutFlags> flags) {
            super(flags);
        }
    }


    // --------------------------------------------------------------------------------


    class EmptyPutFlagSet extends AbstractFlagSet.AbstractEmptyFlagSet<PutFlags> implements PutFlagSet {
    }
}
