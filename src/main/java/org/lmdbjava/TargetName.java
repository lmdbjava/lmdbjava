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

import static java.lang.System.getProperty;
import static java.util.Locale.ENGLISH;

/**
 * Determines the name of the target LMDB native library.
 *
 * <p>Users will typically use an LMDB native library that is embedded within the LmdbJava JAR.
 * Embedded libraries are built by a Zig cross-compilation step as part of the release process. The
 * naming convention reflects the Zig target name plus a common filename extension. This simplifies
 * support for future Zig targets (eg with different toolchains etc).
 *
 * <p>Users can set two system properties to override the automatic resolution of an embedded
 * library. Setting {@link #LMDB_NATIVE_LIB_PROP} will force use of that external LMDB library.
 * Setting {@link #LMDB_EMBEDDED_LIB_PROP} will force use of that embedded LMDB library. If both are
 * set, the former property will take precedence. Most users do not need to set either property.
 */
public final class TargetName {

  /**
   * True if the resolved native filename is an external file (conversely false indicates the file
   * should be considered a classpath resource).
   */
  public static final boolean IS_EXTERNAL;

  /**
   * Java system property name that can be set to override the embedded library that will be used.
   * This is likely to be required if automatic resolution fails but the user still prefers to use
   * an LmdbJava-bundled library. This path must include the classpath prefix (usually <code>
   * org/lmdbjava</code>).
   */
  public static final String LMDB_EMBEDDED_LIB_PROP = "lmdbjava.embedded.lib";

  /**
   * Java system property name that can be set to provide a custom path to an external LMDB system
   * library.
   */
  public static final String LMDB_NATIVE_LIB_PROP = "lmdbjava.native.lib";

  /** Resolved target native filename or fully-qualified classpath location. */
  public static final String RESOLVED_FILENAME;

  private static final String ARCH = getProperty("os.arch");
  private static final String EMBED = getProperty(LMDB_EMBEDDED_LIB_PROP);
  private static final String EXTERNAL = getProperty(LMDB_NATIVE_LIB_PROP);
  private static final String OS = getProperty("os.name");

  static {
    IS_EXTERNAL = isExternal(EXTERNAL);
    RESOLVED_FILENAME = resolveFilename(EXTERNAL, EMBED, ARCH, OS);
  }

  private TargetName() {}

  /**
   * Resolves the filename extension of the bundled LMDB library for a given operating system.
   *
   * @param os typically the <code>os.name</code> system property
   * @return extension of the LMDB system library bundled with LmdbJava
   */
  public static String resolveExtension(final String os) {
    return check(os, "Windows") ? "dll" : "so";
  }

  static boolean isExternal(final String external) {
    return external != null && !external.isEmpty();
  }

  static String resolveFilename(
      final String external, final String embed, final String arch, final String os) {
    if (external != null && !external.isEmpty()) {
      return external;
    }

    if (embed != null && !embed.isEmpty()) {
      return embed;
    }

    final String pkg = TargetName.class.getPackage().getName().replace('.', '/');
    return pkg
        + "/"
        + resolveArch(arch)
        + "-"
        + resolveOs(os)
        + "-"
        + resolveToolchain(os)
        + "."
        + resolveExtension(os);
  }

  /**
   * Case insensitively checks whether the passed string starts with any of the candidate strings.
   *
   * @param string the string being checked
   * @param candidates one or more candidate strings
   * @return true if the string starts with any of the candidates
   */
  private static boolean check(final String string, final String... candidates) {
    if (string == null) {
      return false;
    }

    final String strLower = string.toLowerCase(ENGLISH);
    for (final String c : candidates) {
      if (strLower.startsWith(c.toLowerCase(ENGLISH))) {
        return true;
      }
    }
    return false;
  }

  private static String err(final String reason) {
    return reason
        + " (please set system property "
        + LMDB_NATIVE_LIB_PROP
        + " to the path of an external LMDB native library or property "
        + LMDB_EMBEDDED_LIB_PROP
        + " to the name of an LmdbJava embedded"
        + " library; os.arch='"
        + ARCH
        + "' os.name='"
        + OS
        + "')";
  }

  private static String resolveArch(final String arch) {
    if (check(arch, "aarch64")) {
      return "aarch64";
    } else if (check(arch, "x86_64", "amd64")) {
      return "x86_64";
    }
    throw new UnsupportedOperationException(err("Unsupported os.arch"));
  }

  private static String resolveOs(final String os) {
    if (check(os, "Linux")) {
      return "linux";
    } else if (check(os, "Mac OS")) {
      return "macos";
    } else if (check(os, "Windows")) {
      return "windows";
    }
    throw new UnsupportedOperationException(err("Unsupported os.name"));
  }

  private static String resolveToolchain(final String os) {
    return check(os, "Mac OS") ? "none" : "gnu";
  }
}
