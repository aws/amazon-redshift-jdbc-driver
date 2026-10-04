package com.amazon.redshift.plugin;

import com.amazon.redshift.NativeTokenHolder;
import com.amazon.redshift.core.Utils;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.utils.RequestUtils;
// import removed; use RequestUtils for expiration parsing
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;

/**
 * BrokerEndpointTokenAuthPlugin
 *
 * A credentials provider that always retrieves an access token from a configured token/broker endpoint
 * (e.g., Keycloak broker endpoint), instead of accepting a token directly from the client.
 *
 * Parameters:
 *  - token_type (required)       : Type of token being provided to Redshift
 *  - token_url (required)        : URL of the token/broker endpoint to call to fetch the token
 *  - bearer_token (optional)     : If provided, included as Authorization: Bearer <bearer_token> when calling token_url
 *  - token_attribute (optional)  : Name of the JSON attribute containing the token when token_url returns JSON (default "access_token")
 *  - ssl_insecure (optional)     : If true, disables SSL certificate validation for the outbound call
 *
 * Behavior:
 *  - Makes a GET request to token_url, optionally with Authorization header.
 *  - Supports JSON responses by extracting the token from token_attribute and attempts to parse an expiration
 *    using common fields (via TokenExpirationUtils). If no expiration is found, defaults to 900s.
 *  - Supports non-JSON responses by treating the whole body as the token.
 */
public class IdpTokenUrlAuthPlugin extends CommonCredentialsProvider {

    private static final String KEY_TOKEN_TYPE = "token_type";
    private static final String TOKEN_URL = "token_url";
    private static final String TOKEN_URL_BEARER_TOKEN = "bearer_token";
    private static final String TOKEN_ATTRIBUTE = "token_attribute";
    private static final int DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC = 900;
    private static final String DEFAULT_TOKEN_ATTRIBUTE = "access_token";

    private String token_type;
    private String token_url;
    private String bearer_token;
    private String token_attribute;

    public IdpTokenUrlAuthPlugin() {
    }

    @Override
    protected NativeTokenHolder getAuthToken() throws IOException {
        checkRequiredParameters();
        return getBrokerEndpointToken();
    }

    private NativeTokenHolder getBrokerEndpointToken() throws IOException {
        validateURL(token_url);

        try (CloseableHttpClient client = getHttpClient()) {
            HttpGet get = new HttpGet(token_url);
            if (!Utils.isNullOrEmpty(bearer_token)) {
                get.addHeader("Authorization", "Bearer " + bearer_token);
            }

            try (CloseableHttpResponse resp = client.execute(get)) {
                int status = resp.getStatusLine().getStatusCode();
                String content = resp.getEntity() == null ? "" : EntityUtils.toString(resp.getEntity());

                if (RedshiftLogger.isEnable()) {
                    String masked = Utils.isNullOrEmpty(content) ? content : "***masked***";
                    m_log.logDebug("Broker endpoint response status={0}, body={1}", status, masked);
                }

                if (status != 200) {
                    String msg = "IdC token url failed: Unexpected response status: " + status +
                            (Utils.isNullOrEmpty(content) ? "" : ", body: " + content);
                    throw new IOException(msg);
                }

                String body = Utils.isNullOrEmpty(content) ? "" : content.trim();
                if (body.isEmpty()) {
                    throw new IOException("IdC token url failed: Empty response body from broker endpoint.");
                }

                // Strip trailing '%' sometimes appended in logs/echo
                if (body.endsWith("%")) {
                    body = body.substring(0, body.length() - 1).trim();
                }

                Date defaultExpiration = new Date(System.currentTimeMillis() + DEFAULT_IDP_TOKEN_EXPIRY_IN_SEC * 1000L);

                // JSON response path
                if (body.startsWith("{")) {
                    String attr = Utils.isNullOrEmpty(token_attribute) ? DEFAULT_TOKEN_ATTRIBUTE : token_attribute;
                    try {
                        JsonNode json = Utils.parseJson(body);
                        JsonNode tokenNode = json.findValue(attr);
                        if (tokenNode == null || Utils.isNullOrEmpty(tokenNode.asText())) {
                            throw new IOException("IdC token url failed: JSON attribute '" + attr + "' not found or empty in broker response.");
                        }

                        Date expiration = RequestUtils.getExpirationFromJson(json);
                        if (expiration == null) {
                            expiration = defaultExpiration;
                        }
                        return NativeTokenHolder.newInstance(tokenNode.asText(), expiration);
                    } catch (JsonProcessingException e) {
                        throw new IOException("IdC token url failed: Unable to parse JSON from broker response.", e);
                    }
                }

                // Non-JSON: the entire body is the token
                return NativeTokenHolder.newInstance(body, defaultExpiration);
            }
        } catch (GeneralSecurityException e) {
            throw new IOException("Failed to create HTTP client for broker endpoint.", e);
        }
    }

    private void checkRequiredParameters() throws IOException {
        if (Utils.isNullOrEmpty(token_url)) {
            throw new IOException("IdC authentication failed: The token_url must be included in the connection parameters.");
        } else if (Utils.isNullOrEmpty(token_type)) {
            throw new IOException("IdC authentication failed: The token type must be included in the connection parameters.");
        } else if (!Utils.isNullOrEmpty(bearer_token) && Utils.isNullOrEmpty(token_url)) {
            throw new IOException("IdC token url failed: The bearer_token must be included with the token_url connection parameter.");
        }
    }

    @Override
    public void addParameter(String key, String value) {
        super.addParameter(key, value);

        if (KEY_TOKEN_TYPE.equalsIgnoreCase(key)) {
            token_type = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token_type: {0}", token_type);
        } else if (TOKEN_URL.equalsIgnoreCase(key)) {
            token_url = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token_url: {0}", token_url);
        } else if (TOKEN_URL_BEARER_TOKEN.equalsIgnoreCase(key)) {
            bearer_token = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting bearer_token: {0}", bearer_token);
        } else if (TOKEN_ATTRIBUTE.equalsIgnoreCase(key)) {
            token_attribute = value;
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting token_attribute: {0}", token_attribute);
        } else if (KEY_SSL_INSECURE.equalsIgnoreCase(key)) {
            m_sslInsecure = Boolean.parseBoolean(value);
            if (RedshiftLogger.isEnable())
                m_log.logDebug("Setting ssl_insecure: {0}", m_sslInsecure);
        }
    }

    @Override
    public String getPluginSpecificCacheKey() {
        // Cache key considers url, type and whether Authorization bearer is present
        StringBuilder sb = new StringBuilder("BrokerEndpointTokenAuthPlugin:");
        sb.append("url=").append(token_url == null ? "" : token_url).append(";");
        sb.append("type=").append(token_type == null ? "" : token_type).append(";");
        sb.append("bearer=").append(Utils.isNullOrEmpty(bearer_token) ? "none" : "present");
        return sb.toString();
    }
}
