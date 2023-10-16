package com.amazon.redshift.core;

import com.amazon.redshift.logger.LogLevel;
import com.amazon.redshift.logger.RedshiftLogger;
import com.amazon.redshift.Driver;

import java.lang.Math;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class CompressedInputStream extends InputStream {
    private final InputStream wrapped;
    private static final int LZ4_MAX_MESSAGE_SIZE = 16 * 1024;
    private static final int LZ4_RING_BUFFER_SIZE = 64 * 1024;
    // 80KB as per buffer size on server
    private static final int BUFFER_SIZE = 80 * 1024;
    private byte[] buffer;
    private final RedshiftLogger logger;
    private byte[] decompress_buffer;
    private int decompress_buffer_offset;

    private int bytes_before_next_message = 0;
    private int next_byte = 0;
    private int next_empty_byte = 0;

    private long compressedBytesReadFromStream = 0;

    public CompressedInputStream(InputStream in, RedshiftLogger in_logger) {
        wrapped = in;
        logger = in_logger;
        decompress_buffer = null;
        decompress_buffer_offset = 0;
        buffer = new byte[BUFFER_SIZE];
    }

    public int read() throws IOException {
        int readResult;
        do {
            readResult = tryReadMessage();
            if (readResult < 0)
                return readResult;
        } while (readResult == 1);
        bytes_before_next_message--;
        return buffer[next_byte++];
    }

    public long getBytesReadFromStream()
    {
        return compressedBytesReadFromStream;
    }

    static final int MIN_MATCH = 4; // minimum length of a match

    /**
     * Implementation of lz4 decompression. Curently I could not make any library to do stream decompression
     * as is required by LZ4_decompress_safe_continue().
     */
    public static int lz4_decompress(byte[] compressed, int position, int compressedLen, byte[] dest, int dOff, RedshiftLogger logger) throws IOException {
        final int destEnd = dest.length;
        int startOff = dOff;
        compressedLen += position;

        do
        {
            // literals
            final int token = compressed[position++] & 0xFF;
            int literalLen = token >>> 4;

            if (literalLen != 0)
            {
                if (literalLen == 0x0F)
                {
                    byte len;
                    while ((len = compressed[position++]) == (byte) 0xFF)
                    {
                        literalLen += 0xFF;
                    }
                    literalLen += len & 0xFF;
                }
                for (int i = 0; i < literalLen; i++)
                    dest[dOff + i] = compressed[position++];
                dOff += literalLen;
            }

            if (position >= compressedLen)
            {
                break;
            }

            // matches
            int a = compressed[position++] & 0xFF;
            int b = compressed[position++] & 0xFF;
            final int matchDec = (a) | (b << 8);
            assert matchDec > 0;

            int matchLen = token & 0x0F;
            if (matchLen == 0x0F)
            {
                int len;
                while ((len = compressed[position++]) == (byte) 0xFF)
                {
                    matchLen += 0xFF;
                }
                matchLen += len & 0xFF;
            }
            matchLen += MIN_MATCH;

            // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
            final int fastLen = (matchLen + 7) & 0xFFFFFFF8;
            if (matchDec < matchLen || dOff + fastLen > destEnd)
            {
                // overlap -> naive incremental copy
                for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff)
                {
                    dest[dOff] = dest[ref];
                }
                // Note(xformmm): here we should use memcpy instead of byte loop as we do in
                // https://github.com/postgres/postgres/commit/c60e520f
            }
            else
            {
                // no overlap -> arraycopy
                try
                {
                    System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);
                }
                catch (Exception e)
                {
                    if(RedshiftLogger.isEnable())
                    {
                        logger.logInfo("matchDec : " + matchDec);
                        logger.logInfo("matchLen : " + matchLen);

                        Integer initialSourcePosition = dOff - matchDec;
                        Integer initialDestinationPosition = dOff;
                        Integer length = fastLen;
                        Integer lastSourcePosition = initialSourcePosition + length - 1;
                        Integer lastDestinationPosition = initialDestinationPosition + length - 1;

                        logger.logInfo("initialSourcePosition : " + initialSourcePosition);
                        logger.logInfo("initialDestinationPosition : " + initialDestinationPosition);
                        logger.logInfo("length : " + length);
                        logger.logInfo("lastSourcePosition : " + lastSourcePosition);
                        logger.logInfo("lastDestinationPosition : " + lastDestinationPosition);
                        logger.logInfo("buffer length : " + dest.length);
                    }

                    throw e;
                }
                dOff += matchLen;
            }
        } while (position < compressedLen);

        return dOff - startOff;
    }

    /**
     * Ensures that we have at least one byte and checks if it is compressed message
     * returns 1 if caller have to repeat ()
     */
    private int tryReadMessage() throws IOException
    {
        if (bytes_before_next_message == 0)
        {
            if (!readFromNetwork(5))
            {
                if(RedshiftLogger.isEnable())
                {
                    logger.logInfo("Not yet ready to read from network");
                }

                return -1;
            }
            byte msg_type = buffer[next_byte];
            next_byte++; // Consume message type from stream
            int msgSize = ntoh32();
            if (msg_type == 'k' || msg_type == 'z')
            {
                if (RedshiftLogger.isEnable())
                {
                    if (msg_type == 'z')
                        logger.log(LogLevel.DEBUG, "Compression-aware server, Compression acknowledged");
                    else if (msg_type == 'k')
                        logger.log(LogLevel.DEBUG, "Set Compression method");
                }
                /*
                 * SetCompressionMessageType or CompressionAckMessage
                 * We must restart decompression codec and discard rest of the message.
                 */
                if (!readFromNetwork(msgSize))
                {
                    if(RedshiftLogger.isEnable())
                    {
                        logger.logInfo("Not yet ready to read from network");
                    }

                    return -1;
                }
                next_byte += msgSize;

                if (decompress_buffer == null)
                    decompress_buffer = new byte[LZ4_MAX_MESSAGE_SIZE + 2 * LZ4_RING_BUFFER_SIZE];
                decompress_buffer_offset = 0;
                /* We still have bytes_before_next_message == 0 - next packet is coming */
                return 1;
            }
            else if (msg_type == 'm')
            {
                /*
                 * CompressedData
                 * Decompress everything and add data to buffer
                 */
                next_byte--; // return pointer to the beginning of message
                msgSize++; // account message type byte with message

                if (!readFromNetwork(msgSize))
                {
                    if(RedshiftLogger.isEnable())
                    {
                        logger.logInfo("Not yet ready to read from network");
                    }

                    return -1;
                }
                ensureCapacity(LZ4_MAX_MESSAGE_SIZE);
                int decompressSize = lz4_decompress(buffer, next_byte + 5, msgSize - 5,
                        decompress_buffer, decompress_buffer_offset, logger);

                if (decompressSize < 0)
                {
                    if (RedshiftLogger.isEnable())
                    {
                        logger.logError("Decompressed message has a negative size");
                    }

                    return decompressSize; // Error happened
                }

                /* Shift data after current compressed message */
                try
                {
                    if (decompressSize + next_empty_byte - msgSize > buffer.length)
                    {
                        // Reallocate buffer size to avoid overflowing. This is a fallback to prevent errors.
                        Integer bufferSizeMultiplier = ((decompressSize + next_empty_byte - msgSize) / buffer.length) + 1;
                        buffer = Arrays.copyOf(buffer, buffer.length * bufferSizeMultiplier);
                    }

                    System.arraycopy(buffer, next_byte + msgSize, buffer,
                            next_byte + decompressSize, next_empty_byte - next_byte - msgSize);
                }
                catch (Exception e)
                {
                    if (RedshiftLogger.isEnable())
                    {
                        Integer bufferLength = buffer.length;
                        Integer initialSourcePosition = next_byte + msgSize;
                        Integer initialDestinationPosition = next_byte + decompressSize;
                        Integer length = next_empty_byte - next_byte - msgSize + 1;
                        Integer lastSourcePosition = initialSourcePosition + length - 1;
                        Integer lastDestinationPosition = initialDestinationPosition + length - 1;

                        logger.logDebug("next_byte : " + next_byte);
                        logger.logDebug("msgSize : " + msgSize);
                        logger.logDebug("decompressSize : " + decompressSize);
                        logger.logDebug("next_empty_byte : " + next_empty_byte);
                        logger.logDebug("initialSourcePosition : " + initialSourcePosition);
                        logger.logDebug("initialDestinationPosition : " + initialDestinationPosition);
                        logger.logDebug("length : " + length);
                        logger.logDebug("lastSourcePosition : " + lastSourcePosition);
                        logger.logDebug("lastDestinationPosition : " + lastDestinationPosition);
                        logger.logDebug("buffer length : " + bufferLength);
                    }

                    throw e;
                }

                byte[] decompressedData = new byte[decompressSize];

                for (int i = 0; i < decompressSize; i++)
                {
                    decompressedData[i] = decompress_buffer[decompress_buffer_offset];
                }

                /* Fit decompressed data in */
                System.arraycopy(decompress_buffer, decompress_buffer_offset, buffer, next_byte, decompressSize);

                /* Adjust all counters */
                next_empty_byte = next_empty_byte - msgSize + decompressSize;
                decompress_buffer_offset += decompressSize;
                bytes_before_next_message = decompressSize;

                /* shift decompression buffer if necessary */
                if (decompress_buffer_offset >= 2 * LZ4_RING_BUFFER_SIZE)
                {
                    System.arraycopy(decompress_buffer, LZ4_RING_BUFFER_SIZE, decompress_buffer, 0,
                            LZ4_RING_BUFFER_SIZE + LZ4_MAX_MESSAGE_SIZE);
                    decompress_buffer_offset -= LZ4_RING_BUFFER_SIZE;
                }
                return 0;
            }
            else
            {
                next_byte--; // Return message type byte to the stream
                bytes_before_next_message += msgSize + 1; // Scroll through next message
            }
        }
        /* Ensure at least one byte is ready for the client */
        if (!readFromNetwork(1))
        {
            if (RedshiftLogger.isEnable())
            {
                logger.logInfo("Not yet ready to read from network");
            }

            return -1;
        }
        return 0;
    }

    /**
     * Read 32-bit integer in network format.
     * This function assumes 4 bytes in buffer.
     */
    private int ntoh32() {
        return ((buffer[next_byte] & 0xFF) << 24) + ((buffer[next_byte + 1] & 0xFF) << 16)
                + ((buffer[next_byte + 2] & 0xFF) << 8) + (buffer[next_byte + 3] & 0xFF);
    }

    /* Ensures at least min bytes fetched from network  */
    private boolean readFromNetwork(int min) throws IOException {
        while (next_empty_byte - next_byte < min) {
            /* Make some room if we are out of empty space */
            ensureCapacity(min);
            int read = wrapped.read(buffer, next_empty_byte, buffer.length - next_empty_byte);
            if(read > 0)
            {
                compressedBytesReadFromStream += read;
            }
            if (read < 0)
                return false;
            next_empty_byte += read;
        }

        return true;
    }

    /* Prevents buffer overflow when reading on the edge of buffer */
    private void ensureCapacity(int min) {
        if (next_empty_byte + min >= buffer.length) {
            next_empty_byte = next_empty_byte - next_byte;
            for (int i = 0; i < next_empty_byte; i++) {
                buffer[i] = buffer[i + next_byte];
            }
            next_byte = 0;
        }
    }

    public void close() throws IOException {
        wrapped.close();
    }

    @Override
    public int available() throws IOException {
        return Math.min(next_empty_byte - next_byte, bytes_before_next_message);
    }

    @Override
    public long skip(long n) throws IOException {
        int readResult;
        do {
            readResult = tryReadMessage();
            if (readResult < 0)
                return readResult;
        } while (readResult == 1);
        long available = Math.min(available(), n);
        next_byte += available;
        bytes_before_next_message -= available;
        return available;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int readResult;
        do {
            readResult = tryReadMessage();
            if (readResult < 0)
                return readResult;
        } while (readResult == 1);
        int available = Math.min(available(), len);
        System.arraycopy(buffer, next_byte, b, off, available);
        next_byte += available;
        bytes_before_next_message -= available;
        return available;
    }
}
