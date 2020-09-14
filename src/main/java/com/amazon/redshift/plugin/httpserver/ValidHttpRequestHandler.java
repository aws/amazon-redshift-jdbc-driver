package com.amazon.redshift.plugin.httpserver;

import org.apache.http.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Return valid HTML for all requests.
 */
public class ValidHttpRequestHandler implements HttpRequestHandler
{
    private static final String VALID_RESPONSE = "<!DOCTYPE html><html><body>" +
            "<p style=\"font: italic bold 30px Arial,sans-serif; background-color: #fff;" +
            "color:#202c2d;text-shadow:0 1px #808d93,-1px 0 #cdd2d5,-3px 4px #cdd2d5;\">" +
            "Thank you for using Amazon Redshift! You can now close this window.</p>" + "</body></html>";

    @Override
    public void handle(HttpRequest request, HttpResponse response, HttpContext context)
            throws HttpException, IOException
    {
        response.setEntity(new StringEntity(VALID_RESPONSE, StandardCharsets.UTF_8));
        response.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_HTML.withCharset(StandardCharsets.UTF_8).toString());
        response.setStatusCode(HttpStatus.SC_OK);
    }
}
