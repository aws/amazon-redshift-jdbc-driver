package com.amazon.redshift.plugin;

import java.io.IOException;

/**
 * A basic SAML credential provider class. This class can be changed and implemented to work with
 * any desired SAML service provider.
 */
public class BasicSamlCredentialsProvider extends SamlCredentialsProvider
{
    /**
     * Here we are defining a new connection property key called "saml_assertion". This property
     * will be specific to the BasicSamlCredentialsProvider and will be used to provide some
     * information through the connection string.
     * <p>
     * This means that a user wanting to use this credential provider may include the following in
     * the connection string:
     * <p>
     * <code>
     *          jdbc:redshift:iam://[host]:[port]/[database]?saml_assertion=[value]
     * </code>
     * <p>
     * If your implementation requires user input through the connection string, this is how you
     * can define the connection property name. You can add as many new connection properties as
     * needed following the same pattern:
     * <p>
     * <code>
     *          public static final String PROPERTY_NAME = "key_name";
     * </code>
     * <p>
     * The restrictions on "key_name" are:
     * <p>
     *  -   The name must be unique. It can not match any existing connection property key name in
     *      the Redshift JDBC driver. The connection property names are case-insensitive, so even
     *      if the case does not match what is found in the documentation, it is not allowed.
     * <p>
     *  -   The key name may not have any spaces.
     * <p>
     *  -   The key name may only contain the characters [a-z]|[A-Z] or underscore '_'.
     *
     */
    public static final String KEY_SAML_ASSERTION = "saml_assertion";

    /**
     * This field will store the value given with the associated connection property key.
     * <p>
     * If you are adding additional connection property keys, you will need to define additional
     * fields to hold those values.
     */
    private String samlAssertion;

    /**
     * Optional default constructor.
     */
    public BasicSamlCredentialsProvider()
    {
    }

    /**
     * This method is used to get the values associated with different connection string properties.
     * <p>
     * We override it in this custom credentials provider to add a check for any additional
     * connection properties that were added, which are not included in the existing Redshift JDBC
     * driver. It allows us to store these values using the appropriate fields as mentioned above.
     * <p>
     * For any new connection property keys added to this class, add an if-condition to check, if
     * the current key matches the connection property key, store the value associated with the key
     * in the appropriate field.
     * <p>
     * If no new connection property keys are required, you may leave the implementation blank and
     * simply return a call to the parent class implementation.
     * <p>
     * Please see the example below.
     *
     * @param key       A string representing the connection property key.
     * @param value     The value associated with the connection property key.
     */
    @Override
    public void addParameter(String key, String value)
    {
        // The parent class will take care of setting up all other connection properties which are
        // mentioned in the Redshift JDBC driver documentation.
        super.addParameter(key, value);

        // Add if-condition checks for any connection properties which are specific to your
        // implementation of this custom SAML credentials provider.
        if (KEY_SAML_ASSERTION.equalsIgnoreCase(key))
        {
            samlAssertion = value;
        }
    }

    /**
     * This method needs to return the SAML assertion string returned by the specific SAML provider
     * being used for this implementation. How you get this string will depend on the specific SAML
     * provider you are using.
     * <p>
     * This will be used by the SamlCredentialsProvider parent class to get the temporary credentials.
     *
     * @return  The SAML assertion string.
     * @throws  IOException no error as such. It's an overridden method.
     */
    @Override
    protected String getSamlAssertion() throws IOException
    {
        /*
         *  If you wish to make a connection property required, you can check that the associated
         *  field has been populated, and if not, throw an IOException.
         *          if (StringUtils.isNullOrEmpty(samlAssertion))
         *          {
         *              throw new IOException("Missing required property: " + KEY_SAML_ASSERTION);
         *          }
         */
        return samlAssertion;
    }
}
