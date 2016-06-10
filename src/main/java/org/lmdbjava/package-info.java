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
 * <p>
 * The classes in this package are NOT thread safe. In addition, the LMBC C API
 * requires you to respect specific thread rules (eg do not share transactions
 * between threads). LmdbJava does not shield you from these requirements, as
 * doing so would impose locking overhead on use cases that may not require it
 * or have already carefully implemented application threading (as most low
 * latency applications do to optimize the memory hierarchy, core pinning etc).
 * <p>
 * Please note that many methods require a <code>Buffer</code>. Only DIRECT
 * buffers are supported. These methods will generally change the
 * <code>Buffer</code> to point to a different memory address and offset. It is
 * strongly encouraged to re-use the same <code>Buffer</code> instances across
 * method invocations, as this will substantially decrease latency by avoiding
 * the buffer construction, allocation and garbage collection overheads.
 * <p>
 * In general this package closely approximates the C API. However several
 * "convenience" methods are offered. Such convenience methods will
 * automatically create suitable transactions, create <code>Buffer</code>
 * objects and so on. These convenience methods are never as efficient as using
 * the methods that more closely approximate the C API. If you are latency
 * sensitive, please use the more efficient (non-convenience) methods.
 */
package org.lmdbjava;
