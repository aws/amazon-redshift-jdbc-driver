/**
 * Copyright 2010-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

/**
 * Class to contain all Redshift JDBC driver wide constants
 * Constants should be organized into logical groups with comments.
 */
public final class RedshiftConstants {

    private RedshiftConstants() {
        throw new AssertionError("RedshiftConstants class should not be instantiated.");
    }

    // Auth plugins names related constants
    public static final String BASIC_JWT_PLUGIN = "com.amazon.redshift.plugin.BasicJwtCredentialsProvider";
    public static final String NATIVE_IDP_AZUREAD_BROWSER_PLUGIN = "com.amazon.redshift.plugin.BrowserAzureOAuth2CredentialsProvider";
    public static final String NATIVE_IDP_OKTA_BROWSER_PLUGIN = "com.amazon.redshift.plugin.BrowserOktaSAMLCredentialsProvider";
    public static final String NATIVE_IDP_OKTA_NON_BROWSER_PLUGIN = "com.amazon.redshift.plugin.BasicNativeSamlCredentialsProvider";
    public static final String IDP_TOKEN_PLUGIN = "com.amazon.redshift.plugin.IdpTokenAuthPlugin";
    public static final String IDP_TOKEN_URL_PLUGIN = "com.amazon.redshift.plugin.IdpTokenUrlAuthPlugin";
    public static final String IDC_PKCE_BROWSER_PLUGIN = "com.amazon.redshift.plugin.BrowserIdcAuthPlugin";

}
