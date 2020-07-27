package com.amazon.redshift;

/**
 * Provided authentication mechanism type enum.
 */
public enum AuthMech
{
    /**
     * Indicates the mechanism type is non-SSL.
     */
    DISABLE,

    /**
     * Indicates that the mechanism type is using non-SSL first and then SSL if non-SSL fails.
     */
    ALLOW,

    /**
     * Indicates that the mechanism type is using SSL first and then non-SSL if SSL fails.
     */
    PREFER,

    /**
     * Indicates the mechanism type is using SSL.
     */
    REQUIRE,

    /**
     * Indicates the mechanism type is using SSL and verify the trusted certificate authority.
     */
    VERIFY_CA,

    /**
     * Indicates the mechanism type is using SSL and verify the trusted certificate authority and
     * the server hostname
     */
    VERIFY_FULL;
}
