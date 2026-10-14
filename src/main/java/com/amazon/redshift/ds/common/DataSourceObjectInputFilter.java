/*
 * Copyright Amazon.com, Inc. or its affiliates.
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.ds.common;

import com.amazon.redshift.util.RedshiftProperties;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Support for reading and writing a Redshift {@code DataSource}'s saved settings.
 *
 * <p>On the normal path, {@link BaseDataSource} writes and reads its fields as
 * block-data primitives (see its {@code writeBaseObject}/{@code readBaseObject}) and
 * never calls {@code readObject}, so there is no object-deserialization sink when a
 * DataSource is read back. The marker and version below tag that primitive format.
 *
 * <p>This class exists only to migrate <b>old</b> DataSources. Older drivers wrote the
 * fields as serialized objects; {@link DataSourceMigration} reads those old bytes once
 * through the {@link ValidatingObjectInputStream} here, which reconstructs only an
 * allowlisted set of types ({@link #ALLOWED_CLASSES}) and refuses anything else. That
 * object read happens only inside the converter, never on the normal read path.
 */
final class DataSourceObjectInputFilter {

  /**
   * Marker written at the start of the primitive format so the reader can confirm the
   * stream is in this format (older object streams do not have it). It is the letters
   * {@code 'R','S','D','S'} packed into an int, not a computed value.
   */
  static final int FORMAT_MAGIC =
      ('R' << 24) | ('S' << 16) | ('D' << 8) | 'S';   // 0x52534453

  /** Version of the primitive field layout, written right after the marker. */
  static final int FORMAT_VERSION = 1;

  /**
   * Per-thread flag, set only by the converter. When true,
   * {@link BaseDataSource#readBaseObject(ObjectInputStream)} reads the old
   * field-by-field layout. {@link DataSourceMigration} sets it only while reading from
   * a stream it built (so the type check still applies); never set on the normal path.
   */
  static final ThreadLocal<Boolean> LEGACY_READ = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  static boolean isLegacyRead() {
    return Boolean.TRUE.equals(LEGACY_READ.get());
  }

  /**
   * The exact types a DataSource's saved fields can hold. Listed by name, not a
   * wildcard: a wildcard could allow unwanted types like {@code java.net.URL}, whose
   * {@code equals}/{@code hashCode} can make a DNS call in a hash map. This is only
   * what the six fields (and {@link java.util.Properties}'s internal form) need.
   */
  static final Set<String> ALLOWED_CLASSES;

  static {
    Set<String> allow = new HashSet<String>(Arrays.asList(
        "java.lang.String",
        "[Ljava.lang.String;",          // serverNames
        "[I",                           // portNumbers
        "java.util.Properties",         // properties (declared type)
        "com.amazon.redshift.util.RedshiftProperties", // concrete runtime type of properties
        "java.util.Hashtable",          // Properties' superclass; its serial form writes keys/values
        // Boxed primitive values that may appear in the properties map. Both the
        // concrete type (Integer, Long, ...) and their abstract superclass Number are
        // listed: deserialization calls resolveClass for each class descriptor in a
        // value's superclass chain, so an Integer value also resolves java.lang.Number.
        "java.lang.Number",
        "java.lang.Integer",
        "java.lang.Long",
        "java.lang.Short",
        "java.lang.Byte",
        "java.lang.Float",
        "java.lang.Double",
        "java.lang.Boolean",
        "java.lang.Character"));
    ALLOWED_CLASSES = Collections.unmodifiableSet(allow);
  }

  /**
   * When the converter reads an old stream, the top object is a whole DataSource,
   * which could be any driver DataSource class. Instead of listing them, the converter
   * allows any driver class ({@code com.amazon.redshift.}) that is a
   * {@code BaseDataSource} (see {@link ValidatingObjectInputStream#resolveClass}). Its
   * fields still go through {@link #ALLOWED_CLASSES}, and other classes are rejected.
   */
  static final String DATASOURCE_PACKAGE_PREFIX = "com.amazon.redshift.";

  /** Fully qualified name of the properties subclass whose UID may vary across releases. */
  static final String REDSHIFT_PROPERTIES_CLASS_NAME = RedshiftProperties.class.getName();

  private DataSourceObjectInputFilter() {
  }

  /**
   * An {@link ObjectInputStream} that rebuilds only the classes in
   * {@link #ALLOWED_CLASSES} (plus, during migration, the driver's DataSource classes)
   * and refuses dynamic proxies. Any other class fails with
   * {@link InvalidClassException} before the object is created.
   */
  static final class ValidatingObjectInputStream extends ObjectInputStream {
    private final boolean allowDataSourceTypes;
    private final boolean boundsEnforced;

    ValidatingObjectInputStream(java.io.InputStream in) throws IOException {
      this(in, false);
    }

    ValidatingObjectInputStream(java.io.InputStream in, boolean allowDataSourceTypes)
        throws IOException {
      super(in);
      this.allowDataSourceTypes = allowDataSourceTypes;
      this.boundsEnforced = installResourceLimitsIfAvailable(this);
    }

    /**
     * True if a JDK serialization filter bounding array/refs/depth was installed. When
     * false (the filter API is absent on this JVM), object deserialization is not
     * size-bounded, so the converter refuses to read an old object-format stream rather
     * than risk an oversized-allocation on hostile input.
     */
    boolean boundsEnforced() {
      return boundsEnforced;
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {
      String name = desc.getName();
      if (ALLOWED_CLASSES.contains(name)) {
        return super.resolveClass(desc);
      }
      // Migration only: allow the driver's own DataSource class (the whole stored
      // object). Look it up with initialize=false so the type check does not run its
      // static setup, and return that same class (calling super could resolve the name
      // to a different class). The package-prefix check means an unrelated name is
      // never loaded. Fields inside still go through ALLOWED_CLASSES above.
      if (allowDataSourceTypes && name.startsWith(DATASOURCE_PACKAGE_PREFIX)) {
        Class<?> cls;
        try {
          cls = loadClassNoInit(name);
        } catch (ClassNotFoundException e) {
          throw new InvalidClassException(
              "Unexpected class in DataSource stream", name);
        }
        if (BaseDataSource.class.isAssignableFrom(cls)) {
          return cls; // the class we validated; the JVM initializes it when instantiated
        }
        throw new InvalidClassException(
            "Unexpected non-DataSource driver class in migration stream", name);
      }
      // Not allowlisted and not an allowed driver DataSource type: reject. Log the
      // blocked class name at FINE so a rejected read/conversion can be diagnosed.
      java.util.logging.Logger.getLogger(DataSourceObjectInputFilter.class.getName())
          .log(java.util.logging.Level.FINE,
              "Rejected unexpected class in DataSource stream: {0}", name);
      throw new InvalidClassException(
          "Unexpected class in DataSource stream", name);
    }

    @Override
    protected Class<?> resolveProxyClass(String[] interfaces)
        throws IOException, ClassNotFoundException {
      throw new InvalidClassException("Unexpected dynamic proxy in DataSource stream");
    }

    @Override
    protected ObjectStreamClass readClassDescriptor()
        throws IOException, ClassNotFoundException {
      ObjectStreamClass fromStream = super.readClassDescriptor();
      // Migration only: older drivers did not declare a serialVersionUID on
      // RedshiftProperties or the DataSource classes, so each release baked in the value
      // Java computed from the class shape, and those differ across releases. Reading an
      // old DataSource can fail that UID check even though the fields are the same. We
      // can safely read it by using the current class's descriptor when the stream's
      // serialized field list matches the current class's (same names and types) --
      // only the UID differs. This covers RedshiftProperties and the DataSource classes
      // (whose connection fields are written as block data, not descriptor fields). If
      // the field shapes genuinely differ, we fall through to the normal UID-checked
      // read rather than risk misreading. Applies only in the converter; resolveClass
      // still gates which classes are allowed at all.
      if (allowDataSourceTypes && fromStream != null) {
        Class<?> local = localClassForUidTolerantRead(fromStream.getName());
        if (local != null) {
          ObjectStreamClass localDesc = ObjectStreamClass.lookup(local);
          if (localDesc != null && sameFields(fromStream, localDesc)) {
            return localDesc;
          }
        }
      }
      return fromStream;
    }

    /** True if the two descriptors have the same serialized fields (names and types). */
    private static boolean sameFields(ObjectStreamClass a, ObjectStreamClass b) {
      java.io.ObjectStreamField[] fa = a.getFields();
      java.io.ObjectStreamField[] fb = b.getFields();
      if (fa.length != fb.length) {
        return false;
      }
      // Compare by name+type; field order in the descriptor is normalized by the JVM.
      java.util.Map<String, Class<?>> mb = new java.util.HashMap<String, Class<?>>();
      for (java.io.ObjectStreamField f : fb) {
        mb.put(f.getName(), f.getType());
      }
      for (java.io.ObjectStreamField f : fa) {
        Class<?> t = mb.get(f.getName());
        if (t == null || t != f.getType()) {
          return false;
        }
      }
      return true;
    }

    /**
     * Returns the current driver class to read a stream descriptor against, ignoring
     * its UID, or {@code null} if the descriptor should be read normally. Only
     * RedshiftProperties and driver-owned {@link BaseDataSource} subclasses qualify;
     * all have no own serialized fields, so ignoring the stream UID is data-safe.
     */
    private static Class<?> localClassForUidTolerantRead(String name) {
      if (REDSHIFT_PROPERTIES_CLASS_NAME.equals(name)) {
        return RedshiftProperties.class;
      }
      if (name.startsWith(DATASOURCE_PACKAGE_PREFIX)) {
        try {
          Class<?> cls = loadClassNoInit(name);
          if (BaseDataSource.class.isAssignableFrom(cls)) {
            return cls;
          }
        } catch (ClassNotFoundException notADriverClass) {
          return null;
        }
      }
      return null;
    }

    /**
     * Loads {@code name} without running its static setup, trying the driver's own
     * classloader first and then the thread context classloader. The fallback matters
     * in managed environments (app servers, JNDI providers) where the driver jar and
     * the concrete DataSource class can be loaded by different classloaders -- the same
     * reason normal deserialization uses the caller's loader.
     */
    private static Class<?> loadClassNoInit(String name) throws ClassNotFoundException {
      try {
        return Class.forName(name, false, DataSourceObjectInputFilter.class.getClassLoader());
      } catch (ClassNotFoundException notOnDriverLoader) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        if (tccl == null) {
          throw notOnDriverLoader;
        }
        return Class.forName(name, false, tccl);
      }
    }
  }

  /** Largest array we allow inside one DataSource. serverNames and portNumbers are tiny. */
  static final int MAX_ARRAY_LENGTH = 4096;
  /** Most objects we allow in one DataSource. */
  static final int MAX_REFERENCES = 4096;
  /** Deepest nesting we allow in one DataSource. */
  static final int MAX_DEPTH = 16;

  /**
   * Adds size limits to the read stream: biggest array, object count, and nesting
   * depth. A bad stream can claim a huge array size, and Java allocates it before
   * reading the elements, wasting memory. Uses Java's built-in filter, which exists on
   * Java 9+ ({@code java.io.ObjectInputFilter}) and 8u121+
   * ({@code sun.misc.ObjectInputFilter}); called by reflection so one jar runs on all.
   *
   * @return {@code true} if a size-limiting filter was installed; {@code false} if the
   *         filter API is unavailable on this JVM. The converter uses this to refuse an
   *         old object-format migration it could not bound (see
   *         {@link #newMigrationInputStream(byte[])}).
   */
  private static boolean installResourceLimitsIfAvailable(ObjectInputStream ois) {
    // Same limit string on Java 9+ and 8u121+.
    final String pattern = "maxarray=" + MAX_ARRAY_LENGTH
        + ";maxrefs=" + MAX_REFERENCES
        + ";maxdepth=" + MAX_DEPTH;
    // Prefer the Java 9+ class; fall back to the 8u121+ one.
    for (String pkg : new String[] { "java.io", "sun.misc" }) {
      final Class<?> filterClass;
      final Class<?> configClass;
      try {
        filterClass = Class.forName(pkg + ".ObjectInputFilter");
        configClass = Class.forName(pkg + ".ObjectInputFilter$Config");
      } catch (ClassNotFoundException apiAbsent) {
        continue; // Not on this Java; try the next, else return false.
      }
      try {
        Object ours = configClass.getMethod("createFilter", String.class).invoke(null, pattern);

        // The stream may already have a filter (e.g. -Djdk.serialFilter), and Java
        // will not let us set a second one. If so, combine them: reject if either
        // rejects. Otherwise just set ours. Get/set differ by Java: instance methods
        // on the stream (9+), or static methods on Config that take the stream (8).
        java.lang.reflect.Method instanceGet = optionalMethod(
            ObjectInputStream.class, "getObjectInputFilter");
        final Object existing;
        if (instanceGet != null) {
          existing = instanceGet.invoke(ois);
        } else {
          existing = configClass
              .getMethod("getObjectInputFilter", ObjectInputStream.class).invoke(null, ois);
        }

        Object toInstall = (existing == null)
            ? ours
            : composeFilters(filterClass, ours, existing);

        java.lang.reflect.Method instanceSet = optionalMethod(
            ObjectInputStream.class, "setObjectInputFilter", filterClass);
        if (instanceSet != null) {
          instanceSet.invoke(ois, toInstall);
        } else {
          configClass.getMethod("setObjectInputFilter", ObjectInputStream.class, filterClass)
              .invoke(null, ois, toInstall);
        }
        return true; // installed
      } catch (Exception installFailed) {
        // Filter class present but setting it failed. Log at FINE; treat as not
        // installed so the converter fails closed rather than reading unbounded.
        java.util.logging.Logger.getLogger(DataSourceObjectInputFilter.class.getName())
            .log(java.util.logging.Level.FINE,
                "Could not install DataSource deserialization resource limits via "
                + pkg + ".ObjectInputFilter.",
                installFailed);
        return false;
      }
    }
    return false; // filter API not present on this JVM
  }

  /**
   * Combines two filters: runs ours first, then the existing one. Rejects if either
   * rejects, so it only gets stricter. Built with a {@link java.lang.reflect.Proxy} so
   * it fits whichever filter type ({@code java.io} or {@code sun.misc}) Java provides.
   */
  private static Object composeFilters(Class<?> filterClass, Object ours, Object existing) {
    final java.lang.reflect.Method check = findCheckInput(filterClass);
    final Object reject = rejectedConstant(filterClass);
    if (reject == null) {
      // Without the REJECTED constant we cannot make our size limits authoritative over
      // the existing filter, so we cannot claim bounds are enforced. Fail: the caller
      // treats this as "not installed" and the converter fails closed rather than doing
      // an unbounded object read.
      throw new IllegalStateException(
          "cannot resolve ObjectInputFilter.Status.REJECTED to compose size limits");
    }
    return java.lang.reflect.Proxy.newProxyInstance(
        filterClass.getClassLoader(),
        new Class<?>[] { filterClass },
        (proxy, method, args) -> {
          if (!"checkInput".equals(method.getName())) {
            // Object methods (equals/hashCode/toString) — answer from the proxy itself.
            switch (method.getName()) {
              case "equals":   return proxy == args[0];
              case "hashCode": return System.identityHashCode(proxy);
              case "toString": return "ComposedObjectInputFilter";
              default:         return null;
            }
          }
          Object ourStatus = check.invoke(ours, args[0]);
          if (reject.equals(ourStatus)) {
            return ourStatus; // our limits said no
          }
          Object existingStatus = check.invoke(existing, args[0]);
          if (reject.equals(existingStatus)) {
            return existingStatus; // the existing filter said no
          }
          // Neither said no: use the existing filter's answer (allow or "not sure").
          return existingStatus;
        });
  }

  /** Returns the named public method if it exists on {@code owner}, else {@code null}. */
  private static java.lang.reflect.Method optionalMethod(
      Class<?> owner, String name, Class<?>... paramTypes) {
    try {
      return owner.getMethod(name, paramTypes);
    } catch (NoSuchMethodException absent) {
      return null;
    }
  }

  private static java.lang.reflect.Method findCheckInput(Class<?> filterClass) {
    for (java.lang.reflect.Method m : filterClass.getMethods()) {
      if ("checkInput".equals(m.getName()) && m.getParameterTypes().length == 1) {
        return m;
      }
    }
    throw new IllegalStateException("ObjectInputFilter.checkInput not found");
  }

  /** Finds the filter's {@code Status.REJECTED} value, or null if it cannot be found. */
  private static Object rejectedConstant(Class<?> filterClass) {
    try {
      Class<?> status = Class.forName(filterClass.getName() + "$Status");
      for (Object c : status.getEnumConstants()) {
        if ("REJECTED".equals(c.toString())) {
          return c;
        }
      }
    } catch (ClassNotFoundException ignored) {
      // No Status type here; the caller treats null as "cannot tell if it was rejected".
    }
    return null;
  }

  /**
   * Opens a validating stream over {@code bytes} that also allows the driver's own
   * DataSource classes. Used only by the migration converter.
   */
  static ValidatingObjectInputStream newMigrationInputStream(byte[] bytes) throws IOException {
    return new ValidatingObjectInputStream(new ByteArrayInputStream(bytes), true);
  }

  /**
   * Reads the six fields in the old one-after-another layout from {@code in}. Used only
   * by the converter, with a validating stream it built. Order matches
   * {@code writeBaseObject}: serverNames, databaseName, user, password, portNumbers,
   * properties.
   */
  static Object[] readLegacyInline(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    Object serverNames  = in.readObject();
    Object databaseName = in.readObject();
    Object user         = in.readObject();
    Object password     = in.readObject();
    Object portNumbers  = in.readObject();
    Object properties   = in.readObject();
    return new Object[] { serverNames, databaseName, user, password, portNumbers, properties };
  }

  /**
   * Signals that a stream is not in the current primitive format -- typically an older
   * object stream. {@link BaseDataSource#readBaseObject(ObjectInputStream)} throws it
   * when the format marker is absent/wrong, and the converter catches it by type to
   * fall back to the old object-format read. It is an {@link InvalidClassException}, so
   * a caller that does not catch it specifically still sees a clear message.
   */
  static final class WrongFormatException extends InvalidClassException {
    private static final long serialVersionUID = 1L;

    WrongFormatException(String reason) {
      super(reason);
    }
  }
}
