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

/**
 * Lightning Memory Database (LMDB) for Java (LmdbJava).
 *
 * <p>
 * LmdbJava is intended for extremely low latency use cases. Users are required
 * to understand and comply with the LMDB C API contract (eg handle usage
 * patterns, thread binding, process rules).
 *
 * <p>
 * Priorities:
 * <ol>
 * <li>Minimize latency, particularly on any critical path (see below)</li>
 * <li>Preserve the LMDB C API model as far as practical</li>
 * <li>Apply Java idioms only when not in conflict with the above</li>
 * <li>Fully encapsulate (hide) the native call library and patterns</li>
 * <li>Don't require runtime dependencies beyond the native call library</li>
 * <li>Support official JVMs running on typical 64-bit operating systems</li>
 * <li>Prepare for Java 9 (eg Unsafe, native call technology roadmap etc)</li>
 * </ol>
 *
 * <p>
 * Critical paths of special latency focus:
 * <ul>
 * <li>Releasing and renewing a read-only transaction</li>
 * <li>Any operation that uses a cursor</li>
 * </ul>
 *
 * <p>
 * The classes in this package are NOT thread safe. In addition, the LMBC C API
 * requires you to respect specific thread rules (eg do not share transactions
 * between threads). LmdbJava does not shield you from these requirements, as
 * doing so would impose locking overhead on use cases that may not require it
 * or have already carefully implemented application threading (as most low
 * latency applications do to optimize the memory hierarchy, core pinning etc).
 *
 * <p>
 * Most methods in this package will throw a standard Java exception for failing
 * preconditions (eg {@link NullPointerException} if a mandatory argument was
 * missing) or a subclass of {@link LmdbException} for precondition or LMDB C
 * failures. The majority of LMDB exceptions indicate an API usage or
 * {@link Env} configuration issues, and as such are typically unrecoverable.
 */
package org.lmdbjava;
