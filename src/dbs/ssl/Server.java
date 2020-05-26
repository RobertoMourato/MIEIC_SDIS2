package dbs.ssl;

import dbs.Peer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.*;

public class Server extends NetworkManager {
    private final Selector selector;
    private final Peer peer;
    private final SSLContext context;
    Future<AsynchronousSocketChannel> acceptFuture;
    AsynchronousServerSocketChannel asyncSSC;

    public Server(Peer peer, String address, Integer port) throws Exception {
        this.peer = peer;
        this.context = SSLContext.getInstance("TLSv1.2");

        initManagers(context, false);
        SSLEngine engine = this.context.createSSLEngine(address, port);

        setupPeer(engine, false);
        this.selector = SelectorProvider.provider().openSelector();

        initSocketChannel(address, port);
    }

    private void initSocketChannel(String address, Integer port) throws IOException {
        asyncSSC = AsynchronousServerSocketChannel.open();
        asyncSSC.bind(new InetSocketAddress(address, port));
        //asyncSSC.register(this.selector, SelectionKey.OP_ACCEPT);
    }

    public void read() throws Exception {
        acceptFuture = asyncSSC.accept();

        AsynchronousSocketChannel channel = acceptFuture.get();
        SSLEngine engine = configureSSLEngine();

        handshake(channel, engine);

        System.out.println("SERVER FINISHED HANDSHAKE");

        externalEncryptedBuffer.clear();

        Future<Integer> bytesRead = acceptFuture.get().read(externalEncryptedBuffer);
        ByteBuffer message = ByteBuffer.allocate(30000);
        if (bytesRead.get() > 0) {
            externalEncryptedBuffer.flip();

            while (externalEncryptedBuffer.hasRemaining()) {
                externalApplicationBuffer.clear();
                SSLEngineResult result = engine.unwrap(externalEncryptedBuffer, externalApplicationBuffer);
                if (!handleREADStatus(result, message, engine)) {
                    if (message.position() != 0){
                        byte[] arr = new byte[message.position()];
                        message.rewind();
                        message.get(arr);
                        this.peer.executor.execute(() -> {
                            this.peer.handleMessage(arr);
                        });
                    }
                    return;
                }
            }

            if (message.position() != 0) {
                byte[] arr = new byte[message.position()];
                message.rewind();
                message.get(arr);
                this.peer.executor.execute(() -> {
                    this.peer.handleMessage(arr);
                });
            }

        } else if (bytesRead.get() < 0) {
            engine.closeInbound();

            closure(channel, engine);
        }
    }

    private Boolean handleREADStatus(SSLEngineResult result, ByteBuffer message, SSLEngine engine) throws Exception {
        ByteBuffer aux;
        AsynchronousSocketChannel channel = this.acceptFuture.get();

        switch (result.getStatus()) {
            case OK:
                externalApplicationBuffer.flip();
                message.put(externalApplicationBuffer);

                break;

            case BUFFER_OVERFLOW:
                aux = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize() + this.externalApplicationBuffer.position());
                aux.flip();
                aux.put(this.externalApplicationBuffer);
                this.externalApplicationBuffer = aux;

                break;

            case BUFFER_UNDERFLOW:
                if(engine.getSession().getPacketBufferSize() > this.externalEncryptedBuffer.capacity()) {
                    aux = ByteBuffer.allocate(this.externalEncryptedBuffer.position() + engine.getSession().getApplicationBufferSize());
                    aux.flip();
                    aux.put(this.externalEncryptedBuffer);
                    this.externalEncryptedBuffer = aux;
                } else {
                    this.externalEncryptedBuffer.clear();
                }

                break;

            case CLOSED:
                closure(channel, engine);
                return false;

            default:
                throw new Exception();
        }
        return true;
    }

    private SSLEngine configureSSLEngine() throws SSLException {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.beginHandshake();
        return engine;
    }
}