/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * This class provides a wrapper around a gettext message catalog that can provide a localized
 * version of error messages. The caller provides a message String in the standard
 * java.text.MessageFormat syntax and any arguments it may need. The returned String is the
 * localized version if available or the original if not.
 */
public class GT {

  private static final GT _gt = new GT();
  private static final Object[] noargs = new Object[0];

  public static String tr(String message, Object... args) {
    return _gt.translate(message, args);
  }

  private ResourceBundle bundle;

  private GT() {
    try {
      //JCP! if mvn.project.property.redshift.jdbc.spec < "JDBC4.1"
//JCP>       bundle = ResourceBundle.getBundle("com.amazon.redshift.translation.messages");
      //JCP! else
      bundle = ResourceBundle.getBundle("com.amazon.redshift.translation.messages", Locale.getDefault(Locale.Category.DISPLAY));
      //JCP! endif
    } catch (MissingResourceException mre) {
      // translation files have not been installed
      bundle = null;
    }
  }

  private String translate(String message, Object[] args) {
    if (bundle != null && message != null) {
      try {
        message = bundle.getString(message);
      } catch (MissingResourceException mre) {
        // If we can't find a translation, just
        // use the untranslated message.
      }
    }

    // If we don't have any parameters we still need to run
    // this through the MessageFormat(ter) to allow the same
    // quoting and escaping rules to be used for all messages.
    //
    if (args == null) {
      args = noargs;
    }

    // Replace placeholders with arguments
    //
    if (message != null) {
      message = MessageFormat.format(message, args);
    }

    return message;
  }
}
