[![Maven Build and Deployment](https://github.com/lmdbjava/lmdbjava/workflows/Maven%20Build%20and%20Deployment/badge.svg)](https://github.com/lmdbjava/lmdbjava/actions)
[![codecov](https://codecov.io/gh/lmdbjava/lmdbjava/branch/master/graph/badge.svg)](https://codecov.io/gh/lmdbjava/lmdbjava)
[![Javadocs](http://www.javadoc.io/badge/org.lmdbjava/lmdbjava.svg?color=blue)](http://www.javadoc.io/doc/org.lmdbjava/lmdbjava)
[![Maven Central](https://img.shields.io/maven-central/v/org.lmdbjava/lmdbjava.svg?maxAge=3600)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.lmdbjava%22%20AND%20a%3A%22lmdbjava%22)

# LMDB for Java

[LMDB](http://symas.com/lmdb/) offers:

* Transactions (full ACID semantics)
* Ordered keys (enabling very fast cursor-based iteration)
* Memory-mapped files (enabling optimal OS-level memory management)
* Zero copy design (no serialization or memory copy overhead)
* No blocking between readers and writers
* Configuration-free (no need to "tune" it to your storage)
* Instant crash recovery (no logs, journals or other complexity)
* Minimal file handle consumption (just one data file; not 100,000's like some stores)
* Same-thread operation (LMDB is invoked within your application thread; no compactor thread is needed)
* Freedom from application-side data caching (memory-mapped files are more efficient)
* Multi-threading support (each thread can have its own MVCC-isolated transaction)
* Multi-process support (on the same host with a local file system)
* Atomic hot backups

**LmdbJava** adds Java-specific features to LMDB:

* [Extremely fast](https://github.com/lmdbjava/benchmarks/blob/master/results/20160710/README.md) across a broad range of benchmarks, data sizes and access patterns
* Modern, idiomatic Java API (including iterators, key ranges, enums, exceptions etc)
* Nothing to install (the JAR embeds the latest LMDB libraries for Linux, OS X and Windows)
* Buffer agnostic (Java `ByteBuffer`, Agrona `DirectBuffer`, Netty `ByteBuf`, your own buffer)
* 100% stock-standard, officially-released, widely-tested LMDB C code (no extra C/JNI code)
* Low latency design (allocation-free; buffer pools; optional checks can be easily disabled in production etc)
* Mature code (commenced in 2016) and used for heavy production workloads (eg > 500 TB of HFT data)
* Actively maintained and with a "Zero Bug Policy" before every release (see [issues](https://github.com/lmdbjava/lmdbjava/issues))
* Available from [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.lmdbjava%22%20AND%20a%3A%22lmdbjava%22) and [OSS Sonatype Snapshots](https://oss.sonatype.org/content/repositories/snapshots/org/lmdbjava/lmdbjava)
* [Continuous integration](https://github.com/lmdbjava/lmdbjava/actions) testing on Linux, Windows and macOS with Java 8, 11, 17 and 19

### Performance

![img](https://raw.githubusercontent.com/lmdbjava/benchmarks/master/results/20160710/4-intKey-seq-summary.png)

![img](https://raw.githubusercontent.com/lmdbjava/benchmarks/master/results/20160710/4-intKey-rnd-summary.png)

Full details are in the [latest benchmark report](https://github.com/lmdbjava/benchmarks/blob/master/results/20160710/README.md).

### Documentation

* [Wiki](https://github.com/lmdbjava/lmdbjava/wiki/)
* [Tutorial](https://github.com/lmdbjava/lmdbjava/tree/master/src/test/java/org/lmdbjava/TutorialTest.java)
* [JavaDocs](http://www.javadoc.io/doc/org.lmdbjava/lmdbjava)
* [Change Log](https://github.com/lmdbjava/lmdbjava/wiki/Change-Log)

### Support

We're happy to help you use LmdbJava. Simply
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) if you have
any questions.

### Building

This project uses [Zig](https://ziglang.org/) to cross-compile the LMDB native
library for all supported architectures. To locally build LmdbJava you must
firstly install a recent version of Zig and then execute the project's
[cross-compile.sh](https://github.com/lmdbjava/lmdbjava/tree/master/cross-compile.sh)
script. This only needs to be repeated when the `cross-compile.sh` script is
updated (eg following a new official release of the upstream LMDB library).

If you do not wish to install Zig and/or use an operating system which cannot
easily execute the `cross-compile.sh` script, you can download the compiled
LMDB native library for your platform from a location of your choice and set the
`lmdbjava.native.lib` system property to the resulting file system system
location. Possible sources of a compiled LMDB native library include operating
system package managers, running `cross-compile.sh` on a supported system, or
copying it from the `org/lmdbjava` directory of any recent, officially released
LmdbJava JAR.

### Contributing

Contributions are welcome! Please see the [Contributing Guidelines](CONTRIBUTING.md).

### License

This project is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

This project distribution JAR includes LMDB, which is licensed under
[The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).
