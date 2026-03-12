package com.amazon.redshift.plugin.utils;

import com.amazon.redshift.logger.RedshiftLogger;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

import java.net.URI;
import java.net.URL;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Http Request utils.
 */
public class RequestUtils
{
    private static final String HTTPS_PROTOCOL = "https";
    private static final String HTTPS_PROXY_HOST = "https.proxyHost";
    private static final String HTTPS_PROXY_PORT = "https.proxyPort";
    private static final String HTTP_USE_PROXY = "http.useProxy";
    private static final String HTTP_NON_PROXY_HOSTS = "http.nonProxyHosts";

    private RequestUtils()
    {
    }

    public static StsClient buildSts(String stsEndpoint,
                                     String region,
                                     StsClientBuilder stsClientBuilder,
                                     AwsCredentialsProvider p,
                                     RedshiftLogger log)
            throws Exception {

        ProxyConfiguration proxyConfig = getProxyConfiguration(log);
        if (proxyConfig != null) {
            ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
            httpClientBuilder.proxyConfiguration(proxyConfig);
            stsClientBuilder.httpClientBuilder(httpClientBuilder);
        }

        StsClient stsSvc;

        if (isCustomStsEndpointUrl(stsEndpoint)) {
            stsSvc = stsClientBuilder
                    .credentialsProvider(p)
                    .endpointOverride(URI.create(stsEndpoint))
                    .build();
        }
        else {
            stsClientBuilder.region(Region.of(region));
            stsSvc = stsClientBuilder.credentialsProvider(p).build();
        }

        return stsSvc;
    }

    public static ProxyConfiguration getProxyConfiguration(RedshiftLogger log) {
        boolean useProxy = false;

        try {
            String useProxyStr = System.getProperty(HTTP_USE_PROXY);
            if (useProxyStr != null) {
                useProxy = Boolean.parseBoolean(useProxyStr);
            }
        } catch (Exception ex) {
            if (RedshiftLogger.isEnable()) {
                log.logError(ex);
            }
        }

        if (!useProxy) {
            if (RedshiftLogger.isEnable()) {
                log.logDebug(String.format("useProxy: %s", useProxy));
            }
            return null;
        }

        String proxyHost = System.getProperty(HTTPS_PROXY_HOST);
        String proxyPort = System.getProperty(HTTPS_PROXY_PORT);
        String nonProxyHostsStr = System.getProperty(HTTP_NON_PROXY_HOSTS);

        Set<String> nonProxyHosts = Collections.emptySet();
        if (nonProxyHostsStr != null && !nonProxyHostsStr.isEmpty()) {
            nonProxyHosts = Arrays.stream(nonProxyHostsStr.split("\\|"))
                    .collect(Collectors.toSet());
        }

        if (RedshiftLogger.isEnable()) {
            log.logDebug(
                    String.format("useProxy: %s proxyHost: %s proxyPort:%s nonProxyHosts:%s",
                            useProxy, proxyHost, proxyPort, nonProxyHosts));
        }

        if (proxyHost == null || proxyPort == null) {
            return null;
        }

        return ProxyConfiguration.builder()
                .endpoint(URI.create("https://" + proxyHost + ":" + proxyPort))
                .nonProxyHosts(nonProxyHosts)
                .build();
    }

    private static boolean isCustomStsEndpointUrl(String stsEndpoint) throws Exception {
        boolean isCustomStsEndPoint = false;
        if (stsEndpoint != null
                && !stsEndpoint.isEmpty()) {

            URL aUrl = new URL(stsEndpoint);
            String protocol = aUrl.getProtocol();
            if(protocol != null
                    && protocol.equals(HTTPS_PROTOCOL)) {
                isCustomStsEndPoint = true;
            }
            else {
                throw new Exception("Only https STS URL is supported:" + stsEndpoint);
            }
        }

        return isCustomStsEndPoint;
    }

    /*
     * Checks expiry for credential.
     * Note that this method returns true (i.e. credential is "expired") 1 minute before actual expiry time - This
     * arbitrary buffer has been added to accommodate corner cases and allow enough time for retries if implemented.
     *
     * Returns true (i.e. credential is "expired") if expiry time is null.
     */
    public static boolean isCredentialExpired(Date expiryTime) {
        // We preemptively conclude the credential as expired 1 minute before actual expiry.
        return expiryTime==null || expiryTime.before(new Date(System.currentTimeMillis() + 1000 * 60));
    }

    public static boolean isCredentialExpired(Instant expiryTime) {
        // We preemptively conclude the credential as expired 1 minute before actual expiry.
        return expiryTime == null || expiryTime.isBefore(Instant.now().plusSeconds(60));
    }

    public static Date getExpirationFromJson(JsonNode json) {
        if (json == null) return null;
        JsonNode n = json.findValue("expires_in");
        if (n != null && n.canConvertToLong()) {
            long secs = n.asLong();
            if (secs > 0) return new Date(System.currentTimeMillis() + secs * 1000L);
        }
        for (String k : new String[]{"expires_at", "expiresAt", "expires_on", "expiration", "exp"}) {
            n = json.findValue(k);
            if (n == null) continue;
            if (n.isNumber()) {
                long v = n.asLong();
                if (v > 0) return new Date(v >= 1_000_000_000_000L ? v : v * 1000L);
            }
            if (n.isTextual()) {
                String t = n.asText().trim();
                if (t.isEmpty()) continue;
                try {
                    return new Date(Instant.parse(t).toEpochMilli());
                } catch (DateTimeParseException ignored) {}
                try {
                    long v = Long.parseLong(t);
                    if (v > 0) return new Date(v >= 1_000_000_000_000L ? v : v * 1000L);
                } catch (NumberFormatException ignored) {}
            }
        }
        return null;
    }
}
