/**
 * Copyright 2010-2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
    public static final String IDC_BROWSER_PLUGIN = "com.amazon.redshift.plugin.BrowserIdcAuthPlugin";
    public static final String IDP_TOKEN_PLUGIN = "com.amazon.redshift.plugin.IdpTokenAuthPlugin";

}
