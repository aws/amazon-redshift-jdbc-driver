/*
 * Copyright Amazon.com, Inc. or its affiliates.
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ds.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Converts a Redshift {@code DataSource} saved by an older driver into the format this
 * driver reads. Run it once per stored DataSource.
 *
 * <p>This driver changed how a {@code DataSource} is written and read (see
 * {@link DataSourceObjectInputFilter}), so old bytes are not readable directly. If you
 * have DataSources saved by an older driver (for example in a JNDI/LDAP directory as
 * {@code javaSerializedData}, or in session/failover state), convert them once and
 * save the result back. The read uses the same type check, so anything that is not a
 * real DataSource is rejected.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * byte[] oldBytes = ...;                                 // saved by an older driver
 * byte[] newBytes = DataSourceMigration.migrateToCurrentFormat(oldBytes);
 * // save newBytes back to your store (or re-bind the DataSource)
 * }</pre>
 */
public final class DataSourceMigration {

  private DataSourceMigration() {
  }

  /**
   * Converts saved DataSource bytes into the format this driver reads. Accepts either
   * format: old bytes are re-written in the new format, and bytes already in the new
   * format are returned as an equivalent copy (so it is safe to run over a whole store
   * even if some entries were already converted). Input that is not a real DataSource
   * is rejected.
   *
   * @param serializedDataSource bytes of a Redshift DataSource saved by any driver
   *        version
   * @return the same DataSource in the current format
   * @throws InvalidClassException if the bytes are not a real DataSource
   * @throws IOException if the bytes are broken or cannot be read
   * @throws ClassNotFoundException if a class in the bytes cannot be found
   */
  public static byte[] migrateToCurrentFormat(byte[] serializedDataSource)
      throws IOException, ClassNotFoundException {
    if (serializedDataSource == null) {
      throw new IllegalArgumentException("serializedDataSource must not be null");
    }

    Object dataSource = readCurrentFormat(serializedDataSource);
    if (dataSource == null) {
      // Not the current (primitive) format, so read the old object layout. Set the flag
      // for this thread only, read through a validating stream we built (so the old
      // object fields are still type-checked against the allowlist), and always clear
      // the flag in finally so the normal read path is never affected.
      DataSourceObjectInputFilter.LEGACY_READ.set(Boolean.TRUE);
      try (DataSourceObjectInputFilter.ValidatingObjectInputStream in =
               DataSourceObjectInputFilter.newMigrationInputStream(serializedDataSource)) {
        // Reading the old object layout rebuilds Properties/Hashtable/arrays via
        // readObject, whose array/collection sizes are bounded only by the JDK
        // serialization filter. If that filter is unavailable on this JVM, refuse
        // rather than risk an oversized allocation on hostile input. The filter exists
        // on Java 9+ and Java 8u121+, so run the one-time migration on such a JVM.
        if (!in.boundsEnforced()) {
          java.util.logging.Logger.getLogger(DataSourceMigration.class.getName())
              .log(java.util.logging.Level.WARNING,
                  "Refusing to convert an older-format DataSource: the JDK serialization "
                  + "size-limit filter is unavailable on this JVM. Run the one-time "
                  + "migration on Java 9+ or Java 8u121+.");
          throw new InvalidClassException(
              "Cannot safely convert an older-format DataSource on this JVM because the "
              + "serialization size-limit filter is unavailable. Run the one-time "
              + "migration on Java 9+ or Java 8u121+.");
        }
        dataSource = in.readObject();
      } finally {
        DataSourceObjectInputFilter.LEGACY_READ.remove();
      }
    }

    if (!(dataSource instanceof Serializable) || !(dataSource instanceof BaseDataSource)) {
      throw new InvalidClassException(
          "Input is not a Redshift DataSource: "
          + (dataSource == null ? "null" : dataSource.getClass().getName()));
    }

    // Write it back out with the current driver, which produces the current format.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
      oos.writeObject(dataSource);
    }
    return out.toByteArray();
  }

  /**
   * Tries to read the bytes as a current (primitive) format DataSource. The read uses
   * a validating stream so the outer object can only be a driver DataSource type, and
   * because the current format is primitives, no field is deserialized as an object.
   *
   * @return the DataSource, or {@code null} if the bytes are not the current format
   *         (so the caller tries the old object-format read)
   * @throws InvalidClassException if the bytes are current-format but invalid --
   *         a disallowed outer class, an unsupported format version, or a corrupt
   *         length/count field. (A non-current-format stream is not an error here:
   *         it is caught and reported as {@code null}.)
   */
  private static Object readCurrentFormat(byte[] bytes)
      throws IOException, ClassNotFoundException {
    try (DataSourceObjectInputFilter.ValidatingObjectInputStream in =
             DataSourceObjectInputFilter.newMigrationInputStream(bytes)) {
      return in.readObject();
    } catch (DataSourceObjectInputFilter.WrongFormatException notCurrent) {
      // readBaseObject signals (via this typed exception) that the bytes are not in the
      // current primitive format -- i.e. an older object stream. Return null so the
      // caller falls back to the legacy object read. Any other InvalidClassException
      // (e.g. a disallowed outer class) is a real rejection and propagates.
      return null;
    }
  }
}
