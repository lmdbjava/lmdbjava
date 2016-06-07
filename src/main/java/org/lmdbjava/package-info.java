/**
 * Lightning Memory Database (LMDB) for Java (LmdbJava).
 * <p>
 * LmdbJava is intended for extremely low latency use cases. Users are required
 * to understand and comply with the LMDB C API contract (eg handle usage
 * patterns, thread binding, process rules).
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
 * <p>
 * Critical paths of special latency focus:
 * <ul>
 * <li>Releasing and renewing a read-only transaction</li>
 * <li>Any operation that uses a cursor</li>
 * </ul>
 */
package org.lmdbjava;
