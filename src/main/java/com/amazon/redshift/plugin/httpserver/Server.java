package com.amazon.redshift.plugin.httpserver;

import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.plugin.InternalPluginException;
import org.apache.http.HttpException;
import org.apache.http.HttpServerConnection;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.DefaultBHttpServerConnectionFactory;
import org.apache.http.protocol.*;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * Ad-hoc http server. Listen for one incoming connection or stop after timeout.
 */
public class Server
{
    /**
     * Instance of connection factory.
     */
    private final DefaultBHttpServerConnectionFactory m_connectionFactory;

    /**
     * Instance of http service.
     */
    private final HttpService m_httpService;

    /**
     * IP address.
     */
    private final InetAddress m_ipAddress;

    /**
     * Port number.
     */
    private final int m_port;

    /**
     * Port on witch socket listen.
     * if used random port
     */
    private int local_port;

    /**
     * Instance of Request Handler.
     */
    private final RequestHandler m_handler;

    /**
     * Instance of ServerSocketFactory.
     */
    private final ServerSocketFactory m_socketFactory;

    /**
     * Instance of SocketConfig.
     */
    private final SocketConfig m_defaultSocketConfig;

    /**
     * Instance of ListenerThread.
     */
    private ListenerThread m_listener;
    
    private RedshiftLogger m_log;
    
    private CountDownLatch m_startSignal = null;    
    

    /**
     * Ad-hoc http server.
    *
     * @param port     to listen
     * @param handler  functional callback. put all necessary functionality here
     * @param waitTime how long does server wait for interaction.
     * @param log Redshift logger
     */
    public Server(int port, RequestHandler handler, Duration waitTime,
         				RedshiftLogger log)
    {
    		this.m_log = log;
        this.m_port = port;
        this.m_handler = handler;
        this.m_ipAddress = InetAddress.getLoopbackAddress();
        this.m_socketFactory = ServerSocketFactory.getDefault();
        this.m_defaultSocketConfig = SocketConfig.custom()
            .setBacklogSize(2)
            .setSoKeepAlive(false)
            .setSoTimeout((int) waitTime.toMillis())
            .build();
        this.m_httpService = new HttpService(
            HttpProcessorBuilder.create()
                .add(new ResponseDate())
                .add(new ResponseContent())
                .add(new ResponseConnControl())
                .build(),
            prepareRequestMapper(handler));
        m_connectionFactory = DefaultBHttpServerConnectionFactory.INSTANCE;
    }

    public int getLocalPort() {
        return local_port;
    }

    /**
     * Actual start server to work.
     *
     * @throws IOException all exceptions wrapped in {@link IOException}
     */
    public void listen() throws IOException
    {
        ServerSocket serverSocket = null;
        try
        {
            serverSocket = m_socketFactory.createServerSocket(
                this.m_port,
                this.m_defaultSocketConfig.getBacklogSize(),
                this.m_ipAddress);
            serverSocket.setSoTimeout(m_defaultSocketConfig.getSoTimeout());
            this.local_port = serverSocket.getLocalPort();
            m_listener = new ListenerThread(serverSocket);
            m_startSignal = new CountDownLatch(1);            
            m_listener.start();
            
            // Wait for listener thread to start
            try
            {
              m_startSignal.await();
              m_startSignal = null;
            }
            catch(InterruptedException ie)
            {
              // Ignore
            }
        }
        catch (Throwable ex)
        {
        	if (RedshiftLogger.isEnable())
        		m_log.logError(ex.getMessage());
        	
            if (serverSocket != null)
            {
                serverSocket.close();
            }
            throw ex;
        }

    }

    /**
     * Wait for http callback result.
     * Block execution till result or timeout exited.
     */
    public void waitForResult()
    {
        try
        {
            m_listener.join();
        }
        catch (InterruptedException e)
        {
            // do nothing.
            // resources would be closed by listener thread.
        	if (RedshiftLogger.isEnable())
        		m_log.logError(e);
        }
    }

    /**
     * Stops the server.
     */
    public void stop()
    {
        m_listener.interrupt();
    }

    private UriHttpRequestHandlerMapper prepareRequestMapper(HttpRequestHandler handler)
    {
        UriHttpRequestHandlerMapper mapper = new UriHttpRequestHandlerMapper();
        mapper.register("*", handler);
        return mapper;
    }

    // Copy of apache-core {@link org.apache.http.impl.bootstrap.Worker}
    // and {@link org.apache.http.impl.bootstrap.RequestListener}
    /**
     * Http worker thread.
     */
    public class ListenerThread extends Thread
    {
        private final ServerSocket serverSocket;

        ListenerThread(ServerSocket serverSocket)
        {
            super("http-listener");
            this.serverSocket = serverSocket;
        }

        @Override
        public void run()
        {
            HttpServerConnection conn = null;
            try
            {
               // Signal listener thread started
                m_startSignal.countDown();
                
                final Socket socket = serverSocket.accept();
                socket.setKeepAlive(m_defaultSocketConfig.isSoKeepAlive());
                socket.setTcpNoDelay(m_defaultSocketConfig.isTcpNoDelay());
                if (m_defaultSocketConfig.getRcvBufSize() > 0)
                {
                    socket.setReceiveBufferSize(m_defaultSocketConfig.getRcvBufSize());
                }
                if (m_defaultSocketConfig.getSndBufSize() > 0)
                {
                    socket.setSendBufferSize(m_defaultSocketConfig.getSndBufSize());
                }
                // false by default
                if (m_defaultSocketConfig.getSoLinger() >= 0)
                {
                    socket.setSoLinger(true, m_defaultSocketConfig.getSoLinger());
                }
                conn = m_connectionFactory.createConnection(socket);
                final BasicHttpContext localContext = new BasicHttpContext();
                final HttpCoreContext context = HttpCoreContext.adapt(localContext);
                m_httpService.handleRequest(conn, context);
                localContext.clear();
                conn.close();
                conn = null;
            }
            catch (SocketTimeoutException ex)
            {
                // do nothing. There was no connection during timeout
            	if (RedshiftLogger.isEnable())
            		m_log.logError(ex);
            }
            catch (HttpException | IOException e)
            {
                // Thread can`t throw any checked exceptions from run(), so it needs to be wrapped
                // into RuntimeException.
	            	if (RedshiftLogger.isEnable())
	            		m_log.logError(e);
                throw InternalServerException.wrap(e);
            }
            finally
            {
                try
                {
                    if (conn != null)
                    {
                        conn.shutdown();
                    }
                }
                catch (IOException e)
                {
                    // do nothing
  	            	if (RedshiftLogger.isEnable())
  	            		m_log.logError(e);
                }
                
                try
                {
                    if (!serverSocket.isClosed())
                    {
                        serverSocket.close();
                    }
                }
                catch (IOException e)
                {
                    // do nothing
  	            	if (RedshiftLogger.isEnable())
  	            		m_log.logError(e);
                }
            }
        }
    }
}
