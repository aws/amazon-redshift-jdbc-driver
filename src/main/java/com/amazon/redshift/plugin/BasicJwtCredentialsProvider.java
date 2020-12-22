package com.amazon.redshift.plugin;

import java.io.IOException;

/**
 * A basic JWT credential provider class. This class can be changed and implemented to work with
 * any desired JWT service provider.
 */
public class BasicJwtCredentialsProvider extends JwtCredentialsProvider
{
    /**
     * Optional default constructor.
     */
    public BasicJwtCredentialsProvider()
    {
    }

    /**
     * This method needs to return the JWT string returned by the specific JWT provider
     * being used for this implementation. How you get this string will depend on the specific JWT
     * provider you are using. This method can decode jwt and process any custom claim/tag in it.
     * <p>
     * This will be used by the JwtCredentialsProvider parent class to get the temporary credentials.
     *
     * @param jwt web identity tokens
     * @return  The JWT string.
     * @throws IOException throws exception when required parameters are missing.
     */
    @Override
    protected String processJwt(String jwt) throws IOException 
    {
    		checkRequiredParameters(); 
    		
        return jwt;
    }
}
