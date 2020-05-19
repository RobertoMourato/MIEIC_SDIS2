package dbs.ssl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class NetworkManager {
    ByteBuffer internalApplicationBuffer;
    ByteBuffer internalEncryptedBuffer;
    ByteBuffer externalApplicationBuffer;
    ByteBuffer externalEncryptedBuffer;

    ExecutorService executor = Executors.newSingleThreadExecutor();

    private boolean finishHandshake = false;

    void setupPeer(SSLEngine engine, Boolean isClient) {
        if (isClient)
            engine.setUseClientMode(true);

        SSLSession session = engine.getSession();
        initBuffers(session);
    }

    private void initBuffers(SSLSession session) {
        this.internalApplicationBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
        this.internalEncryptedBuffer = ByteBuffer.allocate(session.getPacketBufferSize());
        this.externalApplicationBuffer = ByteBuffer.allocate(session.getApplicationBufferSize());
        this.externalEncryptedBuffer = ByteBuffer.allocate(session.getPacketBufferSize());
    }

    private void prepareBuffers(SSLEngine engine) {
        int size = engine.getSession().getApplicationBufferSize();

        this.internalApplicationBuffer = ByteBuffer.allocate(size);
        this.externalApplicationBuffer = ByteBuffer.allocate(size);
        this.internalEncryptedBuffer.clear();
        this.externalEncryptedBuffer.clear();
    }

    void handshake(SocketChannel socketChannel, SSLEngine engine) throws Exception {
        SSLEngineResult result;
        HandshakeStatus status;

        prepareBuffers(engine);
        status = engine.getHandshakeStatus();

        while (status != SSLEngineResult.HandshakeStatus.FINISHED && status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            if(this.finishHandshake)
                break;

            switch (status) {
                case NEED_UNWRAP:
                case NEED_UNWRAP_AGAIN:
                    status = handleNeedUnwrap(socketChannel, engine);
                    break;

                case NEED_WRAP:
                    internalEncryptedBuffer.clear();

                    try {
                        result = engine.wrap(this.internalApplicationBuffer, this.internalEncryptedBuffer);
                        status = result.getHandshakeStatus();
                    } catch (SSLException sslException) {
                        sslException.printStackTrace();
                        engine.closeOutbound();
                        status = engine.getHandshakeStatus();
                        break;
                    }

                    handleNeedWrap(socketChannel, engine, result, status);

                    break;

                case NEED_TASK:
                    Runnable task;

                    while ((task = engine.getDelegatedTask()) != null) {
                        executor.execute(task);
                    }
                    status = engine.getHandshakeStatus();

                    break;

                default:
                    throw new Exception();
            }
        }

    }

    private HandshakeStatus handleNeedUnwrap(SocketChannel socketChannel, SSLEngine engine) throws Exception {
        HandshakeStatus status;
        SSLEngineResult result;

        if (socketChannel.read(externalEncryptedBuffer) < 0) {
            status = engine.getHandshakeStatus();
            engine.closeOutbound();
            engine.closeInbound();

            return status;
        }

        externalEncryptedBuffer.flip();

        try {
            result = engine.unwrap(this.externalEncryptedBuffer, this.externalApplicationBuffer);
            this.externalEncryptedBuffer.compact();
            status = result.getHandshakeStatus();
        } catch (SSLException sslException) {
            sslException.printStackTrace();
            engine.closeOutbound();
            status = engine.getHandshakeStatus();

            return status;
        }

        switch (result.getStatus()) {
            case OK:
                break;

            case BUFFER_OVERFLOW:
                if(engine.getSession().getApplicationBufferSize() > this.externalApplicationBuffer.capacity()) {
                    ByteBuffer aux = ByteBuffer.allocate(this.externalApplicationBuffer.position() + engine.getSession().getApplicationBufferSize());
                    aux.flip();
                    aux.put(this.externalApplicationBuffer);
                    this.externalApplicationBuffer = aux;
                } else {
                  this.externalApplicationBuffer.clear();
                }

                break;

            case BUFFER_UNDERFLOW:
                if(engine.getSession().getPacketBufferSize() > this.externalEncryptedBuffer.capacity()) {
                    ByteBuffer aux = ByteBuffer.allocate(this.externalEncryptedBuffer.position() + engine.getSession().getApplicationBufferSize());
                    aux.flip();
                    aux.put(this.externalEncryptedBuffer);
                    this.externalEncryptedBuffer = aux;
                } else {
                    this.externalEncryptedBuffer.clear();
                }

                break;

            case CLOSED:
                if (engine.isOutboundDone()) {
                    this.finishHandshake=true;
                } else {
                    engine.closeOutbound();
                    status = engine.getHandshakeStatus();
                }

                break;

            default:
                throw new Exception();
        }

        return status;
    }

    private void handleNeedWrap(SocketChannel socketChannel, SSLEngine engine, SSLEngineResult result, HandshakeStatus status) throws Exception {
        switch (result.getStatus()) {
            case OK :
                this.internalEncryptedBuffer.flip();

                while (this.internalEncryptedBuffer.hasRemaining()) {
                    socketChannel.write(this.internalEncryptedBuffer);
                }

                break;

            case BUFFER_OVERFLOW:
                ByteBuffer aux = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize() + this.internalEncryptedBuffer.position());
                aux.flip();
                aux.put(this.internalEncryptedBuffer);
                this.internalEncryptedBuffer = aux;

                break;

            case BUFFER_UNDERFLOW:
                if(engine.getSession().getPacketBufferSize() > this.internalApplicationBuffer.capacity()) {
                    aux = ByteBuffer.allocate(this.internalApplicationBuffer.position() + engine.getSession().getApplicationBufferSize());
                    aux.flip();
                    aux.put(this.internalApplicationBuffer);
                    this.internalApplicationBuffer = aux;
                } else {
                    this.externalEncryptedBuffer.clear();
                }

                break;

            case CLOSED:
                this.internalEncryptedBuffer.flip();

                while (this.internalEncryptedBuffer.hasRemaining())
                    socketChannel.write(this.internalEncryptedBuffer);

                this.externalEncryptedBuffer.clear();

                break;

            default:
                throw new Exception();
        }

    }

    void closure(SocketChannel socketChannel, SSLEngine engine) throws Exception {
        engine.closeOutbound();
        handshake(socketChannel, engine);
        socketChannel.close();
    }

    void initManagers(SSLContext context, Boolean isClient) throws UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, KeyManagementException {
        SecureRandom random = new SecureRandom();
        KeyManager[] keyManagers = generateKeyManagers(isClient);
        TrustManager[] trustManagers = generateTrustManagers();

        context.init(keyManagers, trustManagers, random);
    }

    private KeyManager[] generateKeyManagers(Boolean isClient) throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException, UnrecoverableKeyException {
        KeyStore s = generateKeyStore(isClient);
        String algorithm = KeyManagerFactory.getDefaultAlgorithm();
        KeyManagerFactory factory = KeyManagerFactory.getInstance(algorithm);

        factory.init(s, "password".toCharArray());

        return factory.getKeyManagers();
    }

    private KeyStore generateKeyStore(Boolean isClient) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore s = KeyStore.getInstance("JKS");
        String password = "password";
        InputStream input;

        if (isClient)
            input = new FileInputStream("dbs/certificates/client");
        else
            input = new FileInputStream("dbs/certificates/server");

        s.load(input, password.toCharArray());
        input.close();

        return s;
    }

    private TrustManager[] generateTrustManagers() throws CertificateException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KeyStore s = generateTrustStore();
        String algorithm = TrustManagerFactory.getDefaultAlgorithm();
        TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);

        factory.init(s);

        return factory.getTrustManagers();
    }

    private KeyStore generateTrustStore() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        KeyStore s = KeyStore.getInstance("JKS");
        String password = "password";
        InputStream input = new FileInputStream("dbs/certificates/truststore");

        s.load(input, password.toCharArray());
        input.close();

        return s;
    }
}