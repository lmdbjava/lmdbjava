/*
 * Copyright 2016 The LmdbJava Project, http://lmdbjava.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lmdbjava;

/**
 * Operation and DB incompatible, or DB type changed.
 * <p>
 * This can mean:
 * <ul>
 * <li>The operation expects an MDB_DUPSORT / MDB_DUPFIXED database.</li>
 * <li>Opening a named DB when the unnamed DB has MDB_DUPSORT /
 * MDB_INTEGERKEY.</li>
 * <li>Accessing a data record as a database, or vice versa.</li>
 * <li>The database was dropped and recreated with different flags.</li>
 * </ul>
 */
public final class DatabaseIncompatibleException extends LmdbNativeException {

  private static final long serialVersionUID = 1L;
  static final int MDB_INCOMPATIBLE = -30_784;

  DatabaseIncompatibleException() {
    super(MDB_INCOMPATIBLE, "Operation and DB incompatible, or DB type changed");
  }
}
