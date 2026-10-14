/*
 * Copyright 2010-2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package com.amazon.redshift.util;

/**
 * Utility class for safe class loading operations.
 *
 * <p>When a class name comes from untrusted input (for example a JDBC URL
 * parameter like {@code socketFactory}, {@code sslFactory},
 * {@code sslhostnameverifier}, {@code sslpasswordcallback}, or
 * {@code plugin_name}), do not use the single-argument
 * {@link Class#forName(String)}. The single-argument form is equivalent to
 * {@code Class.forName(name, true, callerClassLoader)} — it initializes the
 * class (running any {@code static {}} block) <em>before</em> the caller has a
 * chance to perform an {@code asSubclass(...)} type check. A class with a
 * side-effecting static initializer therefore gets to execute arbitrary code
 * even if it is later rejected as the wrong type.
 *
 * <p>{@link #forName(String, Class, ClassLoader)} defends against this in two
 * layers:
 *
 * <ol>
 *   <li><b>Input-shape validation.</b> The class name must be a well-formed
 *       Java binary name (<a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-13.html#jls-13.1">JLS 13.1</a>)
 *       — a non-empty dot-separated sequence of identifiers. URL-shaped
 *       inputs ({@code jar:file:/proc/self/fd/N!/…}, {@code http://…}),
 *       path-shaped inputs ({@code ../Foo}, {@code pkg/Foo},
 *       {@code pkg\Foo}), array descriptors ({@code [Lcom.example.Foo;},
 *       {@code [I}), and inputs containing whitespace or other
 *       non-identifier characters are rejected as
 *       {@link ClassNotFoundException} <em>before</em> any classloader
 *       interaction. This ensures the loader is never handed a value shaped
 *       to exploit a non-standard classloader that might interpret the
 *       name as something other than a plain identifier.</li>
 *   <li><b>Deferred initialization + type check.</b> The load itself uses
 *       {@code Class.forName(name, false, classLoader)}, which defers static
 *       initialization until the class is actually used (typically when the
 *       caller invokes {@code newInstance()}). The {@code asSubclass(...)}
 *       type check therefore happens before any of the loaded class's code
 *       runs, and a class with the wrong type is rejected without executing
 *       its static initializer.</li>
 * </ol>
 *
 * <p>Note: this is a defense against the "side-effecting static initializer"
 * gadget and against URL-shaped names being smuggled through the class
 * loader, not against an attacker who can place a class on the classpath
 * that legitimately extends the expected type. Customers should still avoid
 * constructing JDBC URLs from untrusted input.
 *
 * <p>This class is <b>NOT</b> public API and is subject to change.
 */
public final class ClassUtils {

  private ClassUtils() {
    // Utility class, no instances.
  }

  /**
   * Safely loads a class by name with class initialization deferred, then
   * verifies it is assignable to {@code expectedClass}.
   *
   * <p>Before touching the classloader, {@code className} is validated as a
   * well-formed Java binary name. Any input containing URL, path, array,
   * or non-identifier characters is rejected with
   * {@link ClassNotFoundException}.
   *
   * @param className     the fully-qualified name of the class to load
   * @param expectedClass the expected superclass or interface
   * @param classLoader   the class loader to use; if {@code null}, the bootstrap
   *                      class loader is used (consistent with
   *                      {@link Class#forName(String, boolean, ClassLoader)})
   * @param <T>           the expected type
   * @return the loaded class, narrowed to {@code ? extends T}
   * @throws ClassNotFoundException if {@code className} is not a well-formed
   *                                Java binary name or the class cannot be found
   * @throws ClassCastException     if the class is not assignable to
   *                                {@code expectedClass}
   */
  public static <T> Class<? extends T> forName(String className, Class<T> expectedClass,
      ClassLoader classLoader) throws ClassNotFoundException {
    if (!isValidBinaryName(className)) {
      throw new ClassNotFoundException(className);
    }
    return Class.forName(className, false, classLoader).asSubclass(expectedClass);
  }

  /**
   * Returns {@code true} iff {@code s} is a well-formed Java binary name per
   * <a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-13.html#jls-13.1">JLS 13.1</a>
   * — a non-empty sequence of identifiers separated by single dots, where
   * each identifier starts with a character for which
   * {@link Character#isJavaIdentifierStart(int)} returns {@code true} and
   * continues with characters for which
   * {@link Character#isJavaIdentifierPart(int)} returns {@code true}.
   *
   * <p>Rejects every input that a legitimate {@code socketFactory},
   * {@code sslFactory}, {@code sslhostnameverifier},
   * {@code sslpasswordcallback}, or {@code plugin_name} value cannot contain:
   *
   * <ul>
   *   <li>URL schemes ({@code jar:}, {@code file:}, {@code http:}) — the
   *       colon fails the identifier-character check.</li>
   *   <li>Path separators ({@code /}, {@code \}) and the nested-JAR
   *       separator ({@code !}) — none are identifier characters.</li>
   *   <li>Array descriptors ({@code [Lcom.example.Foo;}, {@code [I},
   *       {@code [[I}) — the leading {@code [} fails the identifier-start
   *       check.</li>
   *   <li>Whitespace and control characters — none are identifier
   *       characters.</li>
   *   <li>Leading, trailing, or doubled dots ({@code .Foo}, {@code Foo.},
   *       {@code com..Foo}) — an identifier segment cannot be empty.</li>
   *   <li>{@code null} and the empty string.</li>
   * </ul>
   *
   * <p>Uses code-point iteration so supplementary characters (Unicode letters
   * outside the BMP) that are valid Java identifier parts are still accepted.
   */
  static boolean isValidBinaryName(String s) {
    if (s == null || s.isEmpty()) {
      return false;
    }
    boolean expectIdentifierStart = true;
    for (int i = 0; i < s.length();) {
      int cp = s.codePointAt(i);
      if (cp == '.') {
        if (expectIdentifierStart) {
          // Leading dot, or two dots in a row — empty identifier segment.
          return false;
        }
        expectIdentifierStart = true;
      } else if (expectIdentifierStart) {
        if (!Character.isJavaIdentifierStart(cp)) {
          return false;
        }
        expectIdentifierStart = false;
      } else {
        if (!Character.isJavaIdentifierPart(cp)) {
          return false;
        }
      }
      i += Character.charCount(cp);
    }
    // Reject a trailing dot ("com.foo.").
    return !expectIdentifierStart;
  }
}
