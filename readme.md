[![Build Status](https://travis-ci.org/lmdbjava/lmdbjava.svg?branch=master)](https://travis-ci.org/lmdbjava/lmdbjava)
[![Coverage Status](https://coveralls.io/repos/github/lmdbjava/lmdbjava/badge.svg?branch=master)](https://coveralls.io/github/lmdbjava/lmdbjava?branch=master)
[![Dependency Status](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4/badge.svg?style=flat)](https://www.versioneye.com/user/projects/57552e137757a00041b3a6f4)
[![License](https://img.shields.io/hexpm/l/plug.svg?maxAge=2592000)](http://www.apache.org/licenses/LICENSE-2.0.txt)
![Size](https://reposs.herokuapp.com/?path=lmdbjava/lmdbjava)

# LmdbJava

LmdbJava provides an extremely low latency [JNR-FFI](https://github.com/jnr/jnr-ffi)-based
binding to the [LMDB](http://symas.com/mdb/) native library. LMDB is an ultra-fast,
ultra-compact, b-tree ordered, embedded, key-value store developed by Symas for
the OpenLDAP Project. It uses memory-mapped files, so it has the read performance
of a pure in-memory database while still offering the persistence of standard
disk-based databases. It is transactional with full ACID semantics and crash-proof
by design. No journal files. No corruption. No startup time. No dependencies.
Zero-config tuning. LMDB is perfect for large, read-centric, single node workloads
that require extremely low latency and strong operational robustness.

Prospective users might also consider [LMDBJNI](https://github.com/deephacks/lmdbjni),
which uses HawtJNI for its native library binding and supports older JVMs.
LmdbJava is instead focused on the latest available server-grade JVMs.

### License

This project is licensed under the
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

You must separately install the `lmdb` library. LMDB is currently licensed under
[The OpenLDAP Public License](http://www.openldap.org/software/release/license.html).
