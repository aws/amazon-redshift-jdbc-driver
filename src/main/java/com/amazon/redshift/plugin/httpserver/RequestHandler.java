package com.amazon.redshift.plugin.httpserver;

import org.apache.http.*;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import com.amazon.redshift.logger.RedshiftLogger;

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
     * String containing the path for IdC browser authentication.
     */
    public static final String REDSHIFT_IDC_PATH = "/?code=";

    /**
     * String containing the supported POST Rest API.
     */
    private static final String SUPPORTED_METHOD_POST = "POST";

    /**
     * String containing the supported GET Rest API.
     */
    private static final String SUPPORTED_METHOD_GET = "GET";

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
     * Result object.
     */
    private boolean is_valid_result = false;

    protected RedshiftLogger m_log;

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

    public RequestHandler(Function<List<NameValuePair>, Object> requestProcessLogic, RedshiftLogger logger)
    {
        this(requestProcessLogic);
        this.m_log = logger;
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context)
        throws HttpException, IOException
    {
        if (isRequestValid(request))
        {
            if (m_log != null) m_log.logInfo("Received generic response");
            m_result = m_requestProcessLogic.apply(
                URLEncodedUtils.parse(((BasicHttpEntityEnclosingRequest) request).getEntity()));
            is_valid_result = true;
            m_validRequestHandler.handle(request, response, context);
        } else if (isIdcRequestValid(request)) {
            if (m_log != null) m_log.logInfo("Received IdC response");
            String query = extractQuery(request.toString());
            m_result = m_requestProcessLogic.apply(
                URLEncodedUtils.parse(query, java.nio.charset.StandardCharsets.UTF_8));
            is_valid_result = true;
            m_validRequestHandler.handle(request, response, context);
        } else {
            if (m_log != null)
            {
                m_log.logError("Received unknown HTTP request");
                m_log.logInfo("{0}", request.getRequestLine());
                for (Header header : request.getAllHeaders()) {
                    m_log.logInfo(header.getName() + ":" + header.getValue());
                }
            }
            is_valid_result = false;
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
        if (!SUPPORTED_METHOD_POST.equalsIgnoreCase(requestLine.getMethod()))
        {
            return false;
        }
        return requestLine.getUri().startsWith(REDSHIFT_PATH);
    }

    /**
     * Check METHOD and path.
     *
     * @param request {@linkplain HttpRequest}
     */
    private boolean isIdcRequestValid(HttpRequest request)
    {
        RequestLine requestLine = request.getRequestLine();
        if (!SUPPORTED_METHOD_GET.equalsIgnoreCase(requestLine.getMethod()))
        {
            return false;
        }
        return requestLine.getUri().startsWith(REDSHIFT_IDC_PATH);        
    }

    /**
	 * Used to extract the substring from the authorization server HttpRequest request so that we can parse for the authorization code and CSRF state
	 * Custom query parsing message is needed because the returned response is unique from other plugins.
     * 
	 * @param target Entire HttpRequest request from the authorization server
	 * @return Substring of the parameter target that contains the authorization code and CSRF string
	 */
    private static String extractQuery(String target) {
		// Extracts the substring after the first question mark
		int queryIndex = target.indexOf('?');
		if (queryIndex != -1) {
			target = target.substring(queryIndex + 1);
		}
		// Extracts the substring up to the first space
		int spaceIndex = target.indexOf(' ');
		if (spaceIndex != -1) {
			return target.substring(0, spaceIndex + 1);
		}
		return "";
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

    /**
     * @return true if result was found.
     */
    public boolean hasValidResult()
    {
        return is_valid_result;
    }

    /**
     * Resets the result object.
     */
    public void resetValidResult()
    {
        is_valid_result = false;
    }
}
