/**
 * LMDB Low-level interface (LLI).
 * <p>
 * The LLI priorites (in order of importance) are:
 * <ol>
 * <li>Ensure lowest latency operation of the "priority use case" (below)</li>
 * <li>Add simple, zero cost abstractions over LMDB C functions</li>
 * <li>Fully encapsulate (hide) the native call technology and patterns</li>
 * <li>Avoid locks (expect callers to comply with any thread contracts)</li>
 * <li><code>ByteBuffer</code> is the primary buffer type and can be pointed at
 * other memory locations on demand (via unsafe or reflective techniques)</li>
 * <li>Do not add dependencies beyond the native call technology</li>
 * <li>Support standard 64-bit JVMs on typical operating systems</li>
 * <li>Prepare for Java 9 (eg Unsafe, native call technology roadmap etc)</li>
 * </ol>
 * <p>
 * The "priority use case" is:
 * <ul>
 * <li>Linux servers for production</li>
 * <li>Linux, Windows or OS X for development (and potentially production)</li>
 * <li>Read-biased workloads (consider LSM for write-heavy workloads)</li>
 * <li>Cursor-based reads with significant sequential key-value iteration</li>
 * </ul>
 * <p>
 * End users should generally avoid using the LLI, instead preferring a
 * higher-level API that offers a simpler and more idiomatic Java interface.
 */
package org.lmdbjava.core.lli;
