package com.amazon.redshift.plugin.utils;

import java.security.SecureRandom;

/**
 * Random state string generating util.
 */
public class RandomStateUtil
{
    /**
     * Length of the random state string.
     */
    private static final int DEFAULT_STATE_STRING_LENGTH = 10;

    /**
     * Generates random state string 10 char length.
     * 
     * @return generated randomly.
     */
    public static String generateRandomState()
    {
        return generateRandomString();
    }

    /**
     * @return string generated randomly.
     */
    private static String generateRandomString()
    {
        SecureRandom random = new SecureRandom();
        StringBuilder buffer = new StringBuilder(DEFAULT_STATE_STRING_LENGTH);
        for (int i = 0; i < DEFAULT_STATE_STRING_LENGTH; i++)
        {
            buffer.append((char) (random.nextInt(26) + 'a'));
        }
        return buffer.toString();
    }
}
