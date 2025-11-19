package com.amazon.redshift.plugin;

/**
 * Utility class for Azure IDP host operations.
 * Provides common functionality for Azure credential providers.
 */
public class AzureIdpHostUtil {
    
    public static final String MICROSOFT_COMMERCIAL_HOST = "login.microsoftonline.com";
    public static final String MICROSOFT_US_GOV_HOST = "login.microsoftonline.us";
    public static final String MICROSOFT_CHINA_HOST = "login.chinacloudapi.cn";
    
    private AzureIdpHostUtil() {
        throw new AssertionError("Utility class should not be instantiated");
    }
    
    /**
     * Gets the appropriate Azure IDP host based on the partition.
     * The partition value is normalized (trimmed and converted to lowercase) before processing.
     * 
     * @param partition The partition identifier (null, "", "commercial", "us-gov", "cn")
     * @return The corresponding Azure IDP host URL
     * @throws IllegalArgumentException if the partition is invalid
     */
    public static String getIdpHostByPartition(String partition) throws IllegalArgumentException {
        if (partition == null || partition.trim().isEmpty()) {
            return MICROSOFT_COMMERCIAL_HOST; // Default to commercial
        }
        String trimmedPartition = partition.trim().toLowerCase();
        switch (trimmedPartition) {
            case "commercial":
                return MICROSOFT_COMMERCIAL_HOST;
            case "us-gov":
                return MICROSOFT_US_GOV_HOST;
            case "cn":
                return MICROSOFT_CHINA_HOST;
            default:
                throw new IllegalArgumentException("Invalid idp_partition value. Supported values are: null/empty, 'commercial', 'us-gov', 'cn'");
        }
    }
    
    /**
     * Validates the partition value.
     * 
     * @param partition The partition value to validate
     * @throws IllegalArgumentException if the partition is invalid
     */
    public static void validatePartition(String partition) throws IllegalArgumentException {
        try {
            getIdpHostByPartition(partition);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid idp_partition value. Supported values are: null/empty, 'commercial', 'us-gov', 'cn'");
        }
    }
}
