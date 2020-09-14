package com.amazon.redshift.plugin.httpserver;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Return invalid HTML for all requests.
 */
public class InvalidHttpRequestHandler implements HttpRequestHandler
{

    private static final String INVALID_RESPONSE =
        "<!DOCTYPE html><html><body><p>The request could not be understood by the server!</p></body></html>";

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context)
        throws HttpException, IOException
    {
        response.setEntity(new StringEntity(INVALID_RESPONSE,
                StandardCharsets.UTF_8));
        response.setHeader(
            HttpHeaders.CONTENT_TYPE,
            ContentType.TEXT_HTML.withCharset(StandardCharsets.UTF_8).toString());
        response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
    }
}
