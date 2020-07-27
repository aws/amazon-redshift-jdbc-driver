package com.amazon.redshift.plugin.httpserver;

import com.amazon.redshift.plugin.InternalPluginException;

/**
 * Wrapper exception for http server errors.
 * <p>
 * Thread can`t throw any checked exceptions from run(), so it needs to be wrapped into RuntimeException.
 */
public class InternalServerException extends InternalPluginException
{
    /**
     * Constructor.
     *
     * @param cause     Throwable object.
     */
    public InternalServerException(Throwable cause)
    {
        super(cause);
    }

    /**
     * Wrap Exception in this class.
     *
     * @param ex    Exception object.
     *
     * @return instance of this class.
     */
    public static InternalServerException wrap(Exception exceptionToWrap)
    {
        return new InternalServerException(exceptionToWrap);
    }
}
