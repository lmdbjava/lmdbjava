/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2023 The LmdbJava Open Source Project
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class ReferenceUtil {
    /**
     * Ensures that the object referenced by the given reference remains
     * <a href="package-summary.html#reachability"><em>strongly reachable</em></a>,
     * regardless of any prior actions of the program that might otherwise cause
     * the object to become unreachable; thus, the referenced object is not
     * reclaimable by garbage collection at least until after the invocation of
     * this method.
     *
     * <p> Recent versions of the JDK have a nasty habit of prematurely deciding objects are unreachable.
     * see: https://stackoverflow.com/questions/26642153/finalize-called-on-strongly-reachable-object-in-java-8
     * The Java 9 method Reference.reachabilityFence offers a solution to this problem.
     *
     * <p> This method is always implemented as a synchronization on {@code ref}, not as
     * {@code Reference.reachabilityFence} for consistency across platforms and to allow building on JDK 6-8.
     * <b>It is the caller's responsibility to ensure that this synchronization will not cause deadlock.</b>
     *
     * @param ref the reference. If {@code null}, this method has no effect.
     * @see https://github.com/netty/netty/pull/8410
     */
    @SuppressFBWarnings({"ESync_EMPTY_SYNC", "UC_USELESS_VOID_METHOD"})
    public static void reachabilityFence0(Object ref) {
        if (ref != null) {
            synchronized (ref) {
                // Empty synchronized is ok: https://stackoverflow.com/a/31933260/1151521
            }
        }
    }

    private ReferenceUtil() {}
}
