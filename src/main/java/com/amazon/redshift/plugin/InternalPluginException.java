package com.amazon.redshift.plugin;

/**
 * All plugin exceptional state.
 * <p>
 * At the end would be wrapped into {@link java.io.IOException} for API compatibility reason.
 */
public class InternalPluginException extends RuntimeException
{
    /**
     * Constructor.
     *
     * @param message   Error message.
     */
    public InternalPluginException(String message)
    {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param message   Error message.
     * @param cause     Throwable object.
     */
    public InternalPluginException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param cause     Throwable object.
     */
    public InternalPluginException(Throwable cause)
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
    public static InternalPluginException wrap(Exception ex)
    {
        return new InternalPluginException(ex);
    }
}
