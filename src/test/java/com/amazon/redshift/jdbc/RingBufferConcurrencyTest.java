package com.amazon.redshift.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class RingBufferConcurrencyTest {
  private static final int TIMEOUT_SECONDS = 5;

  @Test
  void waitsForRingBufferReaderBeforeSendingAnotherQuery() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      CountDownLatch firstQueryReceived = new CountDownLatch(1);
      CountDownLatch sendFirstRow = new CountDownLatch(1);
      CountDownLatch checkedForOverlappingQuery = new CountDownLatch(1);
      AtomicBoolean overlappingQuery = new AtomicBoolean();
      ExecutorService executor = Executors.newFixedThreadPool(3);

      Future<?> server = executor.submit(() -> {
        try (Socket socket = serverSocket.accept()) {
          DataInputStream input = new DataInputStream(socket.getInputStream());
          DataOutputStream output = new DataOutputStream(socket.getOutputStream());
          receiveStartup(input);
          sendStartupComplete(output);

          assertEquals("select 1", receiveQuery(input));
          firstQueryReceived.countDown();
          assertTrue(sendFirstRow.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
          sendRowDescription(output);
          sendDataRow(output, "1");

          socket.setSoTimeout(500);
          String secondQuery = null;
          try {
            secondQuery = receiveQuery(input);
            overlappingQuery.set(true);
          } catch (SocketTimeoutException expected) {
            // The second query must wait until the first query reaches ReadyForQuery.
          } finally {
            checkedForOverlappingQuery.countDown();
          }

          sendQueryComplete(output, "SELECT 1");
          if (secondQuery == null) {
            socket.setSoTimeout(500);
            try {
              secondQuery = receiveQuery(input);
            } catch (SocketTimeoutException expected) {
              // An interrupted waiter must not start a new protocol operation.
            }
          }
          if (secondQuery != null) {
            assertEquals("select 2", secondQuery);
            sendRowDescription(output);
            sendDataRow(output, "2");
            sendQueryComplete(output, "SELECT 1");
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      String url = "jdbc:redshift://127.0.0.1:" + serverSocket.getLocalPort()
          + "/dev?ssl=false&assumeminserverversion=9.0"
          + "&preferquerymode=simple&enablefetchringbuffer=true";

      try (Connection connection = DriverManager.getConnection(url, "test", "test");
           PreparedStatement firstStatement = connection.prepareStatement("select 1");
           PreparedStatement secondStatement = connection.prepareStatement("select 2")) {
        Future<ResultSet> firstResult = executor.submit(() -> firstStatement.executeQuery());
        assertTrue(firstQueryReceived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        AtomicReference<Thread> secondQueryThread = new AtomicReference<>();
        Future<ResultSet> secondResult = executor.submit(() -> {
          secondQueryThread.set(Thread.currentThread());
          return secondStatement.executeQuery();
        });
        waitUntilWaiting(secondQueryThread, secondResult);
        secondQueryThread.get().interrupt();
        sendFirstRow.countDown();

        try (ResultSet ignored = firstResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          assertTrue(checkedForOverlappingQuery.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
          assertFalse(overlappingQuery.get(),
              "a new query was sent while the prior query's ring-buffer reader was active");
        }
        try {
          secondResult.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          fail("the interrupted query unexpectedly executed");
        } catch (ExecutionException expected) {
          assertTrue(expected.getCause() instanceof SQLException);
        }
        server.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static void waitUntilWaiting(AtomicReference<Thread> thread, Future<?> future)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
    while (System.nanoTime() < deadline) {
      Thread queryThread = thread.get();
      if (queryThread != null && queryThread.getState() == Thread.State.WAITING) {
        return;
      }
      assertFalse(future.isDone(), "the second query unexpectedly completed before the first row");
      Thread.sleep(10);
    }
    fail("the second query did not wait for the active connection operation");
  }

  private static void receiveStartup(DataInputStream input) throws IOException {
    int length = input.readInt();
    input.readFully(new byte[length - Integer.BYTES]);
  }

  private static String receiveQuery(DataInputStream input) throws IOException {
    assertEquals('Q', input.readUnsignedByte());
    int length = input.readInt();
    byte[] query = new byte[length - Integer.BYTES - 1];
    input.readFully(query);
    assertEquals(0, input.readUnsignedByte());
    return new String(query, StandardCharsets.UTF_8);
  }

  private static void sendStartupComplete(DataOutputStream output) throws IOException {
    sendMessage(output, 'R', integer(0));
    sendParameter(output, "server_version", "9.0.0");
    sendParameter(output, "server_encoding", "UTF8");
    sendParameter(output, "client_encoding", "UTF8");
    sendParameter(output, "DateStyle", "ISO");
    sendParameter(output, "TimeZone", "UTC");
    sendParameter(output, "integer_datetimes", "on");
    sendParameter(output, "standard_conforming_strings", "on");
    sendMessage(output, 'K', integers(1234, 5678));
    sendReadyForQuery(output);
  }

  private static void sendParameter(DataOutputStream output, String name, String value)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    writeCString(bytes, name);
    writeCString(bytes, value);
    sendMessage(output, 'S', bytes.toByteArray());
  }

  private static void sendRowDescription(DataOutputStream output) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream fields = new DataOutputStream(bytes);
    fields.writeShort(1);
    writeCString(bytes, "value");
    fields.writeInt(0);
    fields.writeShort(0);
    fields.writeInt(23);
    fields.writeShort(4);
    fields.writeInt(-1);
    fields.writeShort(0);
    sendMessage(output, 'T', bytes.toByteArray());
  }

  private static void sendDataRow(DataOutputStream output, String value) throws IOException {
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream row = new DataOutputStream(bytes);
    row.writeShort(1);
    row.writeInt(encoded.length);
    row.write(encoded);
    sendMessage(output, 'D', bytes.toByteArray());
  }

  private static void sendQueryComplete(DataOutputStream output, String status) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    writeCString(bytes, status);
    sendMessage(output, 'C', bytes.toByteArray());
    sendReadyForQuery(output);
  }

  private static void sendReadyForQuery(DataOutputStream output) throws IOException {
    sendMessage(output, 'Z', new byte[]{'I'});
  }

  private static byte[] integer(int value) throws IOException {
    return integers(value);
  }

  private static byte[] integers(int... values) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    for (int value : values) {
      output.writeInt(value);
    }
    return bytes.toByteArray();
  }

  private static void sendMessage(DataOutputStream output, int type, byte[] payload)
      throws IOException {
    output.writeByte(type);
    output.writeInt(payload.length + Integer.BYTES);
    output.write(payload);
    output.flush();
  }

  private static void writeCString(ByteArrayOutputStream output, String value) throws IOException {
    output.write(value.getBytes(StandardCharsets.UTF_8));
    output.write(0);
  }
}
