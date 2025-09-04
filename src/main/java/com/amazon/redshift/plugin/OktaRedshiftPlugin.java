package com.amazon.redshift.plugin;


import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

import java.io.IOException;
import java.util.Date;

public class OktaRedshiftPlugin extends CommonCredentialsProvider {
    /*
      Parameters for the sso authentication
     */
    private String ssoProfile;
    private String dataViewProfile;
    private String ssoregion;
    private String ssoStartUrl;
    private String ssoAccountId;

    @Override
    protected NativeTokenHolder getAuthToken() throws IOException {
        try {
            if (RedshiftLogger.isEnable()) {
                m_log.logInfo("Starting Okta/AWS SSO authentication flow");
            }

            // Get SSO session credentials
            AWSSessionCredentials ssoCredentials = getSSOCredentials();


            // Assume final role
            //AWSSessionCredentials finalCredentials = assumeTargetRole(ssoCredentials);

            //todo validateCredentials(finalCredentials, "SSO")
            //return createTokenHolder(finalCredentials);
            // below is just for testing
            NativeTokenHolder holder = NativeTokenHolder.newInstance(
                    ssoCredentials.getSessionToken(),
                    new Date(System.currentTimeMillis() + 3600000) // 1 hour expiration
            );

            return holder;

        } catch (Exception e) {
            throw new IOException("Authentication failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void addParameter(String key, String value) {
        switch (key) {
            case "sso_profile":
                this.ssoProfile = value;
                break;
            case "final_profile":
                this.dataViewProfile = value;
                break;
            case "region":
                this.ssoregion = value;
                break;
            case "sso_start_url":
                this.ssoStartUrl = value;
                break;

            default:
                super.addParameter(key, value);
        }
    }

    private AWSSessionCredentials getSSOCredentials() throws IOException {
        ProfileCredentialsProvider provider = new ProfileCredentialsProvider(ssoProfile);
        return (AWSSessionCredentials) provider.getCredentials();
    }

}

