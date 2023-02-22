package com.amazon.redshift.util;

import java.util.Locale;
import java.util.Properties;
import java.util.Enumeration;

public class RedshiftProperties extends Properties {

    /**
     * Creates an empty property list with no default values.
     */
    public RedshiftProperties() {
        super(null);
    }

    /**
     * Creates an empty property list with the specified defaults.
     *
     * @param   defaults   the defaults.
     */
    public RedshiftProperties(Properties defaults) {
        super(defaults);
    }

    /**
     * Creates an empty property list with the specified defaults.
     *
     * @param   defaults   the defaults.
     * @param   info   the input properties that need to be copied.
     * @throws  RedshiftException   RedshiftException
     */
    public RedshiftProperties(Properties info, Properties defaults) throws RedshiftException
    {
        super(defaults);

        if(info!=null)
        {
            // Properties from user come in as a Properties object. Below code block converts them to a RedshiftProperties object and also converting their keys to lowercase.
            Enumeration en = info.propertyNames();

            while (en.hasMoreElements())
            {
                String key = (String) en.nextElement();
                String val = info.getProperty(key);

                if (val == null)
                {
                    throw new RedshiftException(
                            GT.tr("Properties for the driver contains a non-string value for the key ")
                                    + key,
                            RedshiftState.UNEXPECTED_ERROR);
                }

                this.setProperty(key, val);
            }
        }
    }

    /**
     * get value from {Properties}
     * @param key key
     * @return property value
     */
    @Override
    public String getProperty(String key) {
        return super.getProperty(key.toLowerCase(Locale.ENGLISH));
    }

    @Override
    public synchronized Object setProperty(String key, String value)
    {
        return super.setProperty(key.toLowerCase(Locale.ENGLISH), value);
    }
}
