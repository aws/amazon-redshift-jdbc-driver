package com.amazon.redshift.plugin.httpserver;

import org.apache.http.*;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * Post http request handler.
 * Responsible on showing "Complete request" page.
 */
public class RequestHandler implements HttpRequestHandler
{
    /**
     * String containing the path.
     */
    public static final String REDSHIFT_PATH = "/redshift/";

    /**
     * String containing the supported Rest API.
     */
    private static final String SUPPORTED_METHOD = "POST";

    /**
     * Instance of Function.
     */
    private final Function<List<NameValuePair>, Object> m_requestProcessLogic;

    /**
     * Instance of HttpRequestHandler for invalid requests.
     */
    private final HttpRequestHandler m_invalidRequestHandler;

    /**
     * Instance of HttpRequestHandler for valid requests.
     */
    private final HttpRequestHandler m_validRequestHandler;

    /**
     * Result object.
     */
    private Object m_result;

    /**
     * Constructor.
     *
     * @param requestProcessLogic  Function with List of NameValuePair.
     */
    public RequestHandler(Function<List<NameValuePair>, Object> requestProcessLogic)
    {
        this.m_requestProcessLogic = requestProcessLogic;
        this.m_invalidRequestHandler = new InvalidHttpRequestHandler();
        this.m_validRequestHandler = new ValidHttpRequestHandler();
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context)
        throws HttpException, IOException
    {
        if (isRequestValid(request))
        {
            m_result = m_requestProcessLogic.apply(
                URLEncodedUtils.parse(((BasicHttpEntityEnclosingRequest) request).getEntity()));
            m_validRequestHandler.handle(request, response, context);
        }
        else
        {
            m_invalidRequestHandler.handle(request, response, context);
        }
    }

    /**
     * Check METHOD and path.
     *
     * @param request {@linkplain HttpRequest}
     */
    private boolean isRequestValid(HttpRequest request)
    {
        RequestLine requestLine = request.getRequestLine();
        if (!SUPPORTED_METHOD.equalsIgnoreCase(requestLine.getMethod()))
        {
            return false;
        }
        return requestLine.getUri().startsWith(REDSHIFT_PATH);
    }

    /**
     * @return the result object.
     */
    public Object getResult()
    {
        return m_result;
    }

    /**
     * @return true if result is not null.
     */
    public boolean hasResult()
    {
        return m_result != null;
    }
}
