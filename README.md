[![Build Status](https://travis-ci.org/lmdbjava/lmdbjava.svg?branch=master)](https://travis-ci.org/lmdbjava/lmdbjava)
[![Build Status](https://ci.appveyor.com/api/projects/status/0w4yb9ybx22g2pwp?svg=true)](https://ci.appveyor.com/project/benalexau/lmdbjava)
[![codecov](https://codecov.io/gh/lmdbjava/lmdbjava/branch/master/graph/badge.svg)](https://codecov.io/gh/lmdbjava/lmdbjava)
[![Dependency Status](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4)
[![Javadocs](http://www.javadoc.io/badge/org.lmdbjava/lmdbjava.svg?color=blue)](http://www.javadoc.io/doc/org.lmdbjava/lmdbjava)
[![Maven Central](https://img.shields.io/maven-central/v/org.lmdbjava/lmdbjava.svg?maxAge=3600)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.lmdbjava%22%20AND%20a%3A%22lmdbjava%22)

# LMDB for Java

[LMDB](http://symas.com/mdb/) offers:

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
* Modern, idiomatic Java API (including iterators, enums, exceptions etc)
* Nothing to install (the JAR embeds LMDB libraries for Linux, OS X and Windows)
* Buffer agnostic (Java `ByteBuffer`, Agrona `DirectBuffer`, Netty `ByteBuf`, your own buffer)
* 100% stock-standard, officially-released, widely-tested LMDB C code ([no extra](https://github.com/lmdbjava/native) C/JNI code)
* Low latency design (allocation-free; buffer pools; optional checks can be easily disabled in production etc)
* Easy to use (just work through our step-by-step, CI-tested, fully-executable [tutorial](https://github.com/lmdbjava/lmdbjava/tree/master/src/test/java/org/lmdbjava/TutorialTest.java))
* Java 9 ready ([JNR-FFI](https://github.com/jnr/jnr-ffi)-based; `Unsafe` not required etc)
* Available from [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.lmdbjava%22%20AND%20a%3A%22lmdbjava%22) and [OSS Sonatype Snapshots](https://oss.sonatype.org/content/repositories/snapshots/org/lmdbjava/lmdbjava)
* Comprehensive [JavaDocs](http://www.javadoc.io/doc/org.lmdbjava/lmdbjava)
* [Linux](https://travis-ci.org/lmdbjava/lmdbjava), [OS X](https://travis-ci.org/lmdbjava/lmdbjava) and [Windows](https://ci.appveyor.com/project/benalexau/lmdbjava) CI

### Performance

![img](https://raw.githubusercontent.com/lmdbjava/benchmarks/master/results/20160710/4-intKey-seq-summary.png)

![img](https://raw.githubusercontent.com/lmdbjava/benchmarks/master/results/20160710/4-intKey-rnd-summary.png)

Full details are in the [latest benchmark report](https://github.com/lmdbjava/benchmarks/blob/master/results/20160710/README.md).

### Support

We're happy to help you use LmdbJava. Simply
[open a GitHub issue](https://github.com/lmdbjava/lmdbjava/issues) if you have
any questions.

### Contributing

Contributions are welcome! Please see the [Contributing Guidelines](CONTRIBUTING.md).

### License

This project is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

This project distribution JAR includes LMDB, which is licensed under
[The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).
