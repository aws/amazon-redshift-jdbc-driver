/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jre7.sasl;

import com.amazon.redshift.core.RedshiftStream;
import com.amazon.redshift.util.GT;
import com.amazon.redshift.util.RedshiftException;
import com.amazon.redshift.util.RedshiftState;

import com.ongres.scram.client.ScramClient;
import com.ongres.scram.client.ScramSession;
import com.ongres.scram.common.exception.ScramException;
import com.ongres.scram.common.exception.ScramInvalidServerSignatureException;
import com.ongres.scram.common.exception.ScramParseException;
import com.ongres.scram.common.exception.ScramServerErrorException;
import com.ongres.scram.common.stringprep.StringPreparations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ScramAuthenticator {

  private final String user;
  private final String password;
  private final RedshiftStream pgStream;
  private ScramClient scramClient;
  private ScramSession scramSession;
  private ScramSession.ServerFirstProcessor serverFirstProcessor;
  private ScramSession.ClientFinalProcessor clientFinalProcessor;

  private interface BodySender {
    void sendBody(RedshiftStream pgStream) throws IOException;
  }

  private void sendAuthenticationMessage(int bodyLength, BodySender bodySender)
      throws IOException {
    pgStream.sendChar('p');
    pgStream.sendInteger4(Integer.SIZE / Byte.SIZE + bodyLength);
    bodySender.sendBody(pgStream);
    pgStream.flush();
  }

  public ScramAuthenticator(String user, String password, RedshiftStream pgStream) {
    this.user = user;
    this.password = password;
    this.pgStream = pgStream;
  }

  public void processServerMechanismsAndInit() throws IOException, RedshiftException {
    List<String> mechanisms = new ArrayList<>();
    do {
      mechanisms.add(pgStream.receiveString());
    } while (pgStream.peekChar() != 0);
    int c = pgStream.receiveChar();
    assert c == 0;
    if (mechanisms.size() < 1) {
      throw new RedshiftException(
          GT.tr("No SCRAM mechanism(s) advertised by the server"),
          RedshiftState.CONNECTION_REJECTED
      );
    }

    try {
      scramClient = ScramClient
          .channelBinding(ScramClient.ChannelBinding.NO)
          .stringPreparation(StringPreparations.NO_PREPARATION)
          .selectMechanismBasedOnServerAdvertised(mechanisms.toArray(new String[]{}))
          .setup();
    } catch (IllegalArgumentException e) {
      throw new RedshiftException(
          GT.tr("Invalid or unsupported by client SCRAM mechanisms", e),
          RedshiftState.CONNECTION_REJECTED
      );
    }
/*    if (logger.isLoggable(Level.FINEST)) {
      logger.log(Level.FINEST, " Using SCRAM mechanism {0}", scramClient.getScramMechanism().getName());
    } */

    scramSession =
        scramClient.scramSession("*");   // Real username is ignored by server, uses startup one
  }

  public void sendScramClientFirstMessage() throws IOException {
    String clientFirstMessage = scramSession.clientFirstMessage();
//    logger.log(LogLevel.DEBUG, " FE=> SASLInitialResponse( {0} )", clientFirstMessage);

    String scramMechanismName = scramClient.getScramMechanism().getName();
    final byte[] scramMechanismNameBytes = scramMechanismName.getBytes(StandardCharsets.UTF_8);
    final byte[] clientFirstMessageBytes = clientFirstMessage.getBytes(StandardCharsets.UTF_8);
    sendAuthenticationMessage(
        (scramMechanismNameBytes.length + 1) + 4 + clientFirstMessageBytes.length,
        new BodySender() {
          @Override
          public void sendBody(RedshiftStream pgStream) throws IOException {
            pgStream.send(scramMechanismNameBytes);
            pgStream.sendChar(0); // List terminated in '\0'
            pgStream.sendInteger4(clientFirstMessageBytes.length);
            pgStream.send(clientFirstMessageBytes);
          }
        }
    );
  }

  public void processServerFirstMessage(int length) throws IOException, RedshiftException {
    String serverFirstMessage = pgStream.receiveString(length);
//    logger.log(Level.FINEST, " <=BE AuthenticationSASLContinue( {0} )", serverFirstMessage);

    try {
      serverFirstProcessor = scramSession.receiveServerFirstMessage(serverFirstMessage);
    } catch (ScramException e) {
      throw new RedshiftException(
          GT.tr("Invalid server-first-message: {0}", serverFirstMessage),
          RedshiftState.CONNECTION_REJECTED,
          e
      );
    }
/*    if (logger.isLoggable(Level.FINEST)) {
      logger.log(Level.FINEST,
                 " <=BE AuthenticationSASLContinue(salt={0}, iterations={1})",
                 new Object[] { serverFirstProcessor.getSalt(), serverFirstProcessor.getIteration() }
                 );
    } */

    clientFinalProcessor = serverFirstProcessor.clientFinalProcessor(password);

    String clientFinalMessage = clientFinalProcessor.clientFinalMessage();
//    logger.log(Level.FINEST, " FE=> SASLResponse( {0} )", clientFinalMessage);

    final byte[] clientFinalMessageBytes = clientFinalMessage.getBytes(StandardCharsets.UTF_8);
    sendAuthenticationMessage(
        clientFinalMessageBytes.length,
        new BodySender() {
          @Override
          public void sendBody(RedshiftStream pgStream) throws IOException {
            pgStream.send(clientFinalMessageBytes);
          }
        }
    );
  }

  public void verifyServerSignature(int length) throws IOException, RedshiftException {
    String serverFinalMessage = pgStream.receiveString(length);
//    logger.log(Level.FINEST, " <=BE AuthenticationSASLFinal( {0} )", serverFinalMessage);

    try {
      clientFinalProcessor.receiveServerFinalMessage(serverFinalMessage);
    } catch (ScramParseException e) {
      throw new RedshiftException(
          GT.tr("Invalid server-final-message: {0}", serverFinalMessage),
          RedshiftState.CONNECTION_REJECTED,
          e
      );
    } catch (ScramServerErrorException e) {
      throw new RedshiftException(
          GT.tr("SCRAM authentication failed, server returned error: {0}",
              e.getError().getErrorMessage()),
          RedshiftState.CONNECTION_REJECTED,
          e
      );
    } catch (ScramInvalidServerSignatureException e) {
      throw new RedshiftException(
          GT.tr("Invalid server SCRAM signature"),
          RedshiftState.CONNECTION_REJECTED,
          e
      );
    }
  }
}
