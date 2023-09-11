package com.amazon.redshift.plugin;

import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazonaws.util.StringUtils;

import java.io.IOException;
import java.util.Date;

/**
 * A basic credential provider class.
 * This plugin class allows clients to directly provide any auth token that is handled by Redshift.
 */
public class IdpTokenAuthPlugin extends CommonCredentialsProvider {

    private static final String KEY_TOKEN = "token";
    private static final String KEY_TOKEN_TYPE = "token_type";
    private static final int DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC = 900;

    private String token;
    private String token_type;

    public IdpTokenAuthPlugin() {
    }

    /**
     * This overridden method needs to return the auth token provided by the client
     *
     * @return {@link NativeTokenHolder} A wrapper containing auth token and its expiration time information
     * @throws IOException indicating that some required parameter is missing.
     */
    @Override
    protected NativeTokenHolder getAuthToken() throws IOException {
        checkRequiredParameters();

        Date expiration = new Date(System.currentTimeMillis() + DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC * 1000L);
        return NativeTokenHolder.newInstance(token, expiration);
    }

    private void checkRequiredParameters() throws IOException {
        if (StringUtils.isNullOrEmpty(token)) {
            throw new IOException("IdC authentication failed: The token must be included in the connection parameters.");
        } else if (StringUtils.isNullOrEmpty(token_type)) {
            throw new IOException("IdC authentication failed: The token type must be included in the connection parameters.");
        }
    }

    @Override
    public void addParameter(String key, String value) {
        super.addParameter(key, value);

        if (KEY_TOKEN.equalsIgnoreCase(key)) {
            token = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token of length={0}", token.length());
        } else if (KEY_TOKEN_TYPE.equalsIgnoreCase(key)) {
            token_type = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token_type: {0}", token_type);
        }
    }
}
