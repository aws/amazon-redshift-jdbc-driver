package com.amazon.redshift.ssl;

import javax.net.ssl.SSLContext;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

/**
 * Utility class for SSL/TLS operations.
 */
public final class SSLUtil {
    
    private SSLUtil() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Creates a secure SSLContext using the highest available TLS version (1.3 or 1.2).
     * This prevents downgrade attacks by explicitly avoiding weak protocols.
     * 
     * @return SSLContext configured with TLS 1.3 (preferred) or TLS 1.2 (fallback)
     * @throws NoSuchAlgorithmException if neither TLS 1.3 nor TLS 1.2 is available
     */
    public static SSLContext createSecureSSLContext() throws NoSuchAlgorithmException {
        // Try TLS 1.3 first (most secure)
        try {
            return SSLContext.getInstance("TLSv1.3");
        } catch (NoSuchAlgorithmException e) {
            // Fallback to TLS 1.2 if TLS 1.3 is not available
            return SSLContext.getInstance("TLSv1.2");
        }
    }
}
