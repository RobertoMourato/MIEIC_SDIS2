package dbs.ssl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import javax.net.ssl.*;

public class Client extends NetworkManager {
    private final String remoteAddress;
    private final int port;

    private SocketChannel socketChannel;
    private final SSLEngine engine;

    public Client(String remoteAddress, int port) throws Exception  {
        this.remoteAddress = remoteAddress;
        this.port = port;

        SSLContext context = SSLContext.getInstance("TLSv1.2");
        initManagers(context, true);

        this.engine = context.createSSLEngine();
        setupPeer(this.engine, true);
    }

    public void connect() throws Exception {
        this.socketChannel = SocketChannel.open();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.connect(new InetSocketAddress(this.remoteAddress, this.port));

        do {} while(!this.socketChannel.finishConnect());

        this.engine.beginHandshake();
        handshake(this.socketChannel, this.engine);
    }

    public void write(byte[] message) throws Exception {
        prepareBuffersForWrite(message);

        while (this.internalApplicationBuffer.hasRemaining()) {
            this.internalEncryptedBuffer.clear();

            SSLEngineResult result = this.engine.wrap(this.internalApplicationBuffer, this.internalEncryptedBuffer);
            ByteBuffer aux;

            switch (result.getStatus()) {
                case OK:
                    this.internalEncryptedBuffer.flip();

                    while (this.internalEncryptedBuffer.hasRemaining()) {
                        this.socketChannel.write(this.internalEncryptedBuffer);
                    }
                    break;

                case BUFFER_OVERFLOW:
                    aux = ByteBuffer.allocate(this.engine.getSession().getPacketBufferSize() + this.internalEncryptedBuffer.position());
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
                        this.internalApplicationBuffer.clear();
                    }

                    break;

                case CLOSED:
                    closure(this.socketChannel, this.engine);
                    return;

                default:
                    throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
            }
        }
    }

    private void prepareBuffersForWrite(byte[] message) {
        this.internalApplicationBuffer.clear();
        this.internalApplicationBuffer.put(message);
        this.internalApplicationBuffer.flip();
    }

    public void shutdown() throws Exception {
        closure(this.socketChannel, this.engine);
        this.executor.shutdown();
    }
}