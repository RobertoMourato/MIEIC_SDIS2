package dbs.ssl;

import dbs.Peer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import javax.net.ssl.*;

public class Server extends NetworkManager {
    private final Selector selector;
    private final Peer peer;
    private final SSLContext context;

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
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.socket().bind(new InetSocketAddress(address, port));
        serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
    }

    private void read(SocketChannel channel, SSLEngine engine) throws Exception {
        externalEncryptedBuffer.clear();

        int bytesRead = channel.read(externalEncryptedBuffer);
        ByteBuffer message = ByteBuffer.allocate(30000);

        if (bytesRead > 0) {
            externalEncryptedBuffer.flip();

            while (externalEncryptedBuffer.hasRemaining()) {
                externalApplicationBuffer.clear();
                SSLEngineResult result = engine.unwrap(externalEncryptedBuffer, externalApplicationBuffer);
                if (!handleREADStatus(result, message, engine, channel)) {
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

        } else if (bytesRead < 0) {
            engine.closeInbound();

            closure(channel, engine);
        }
    }

    private Boolean handleREADStatus(SSLEngineResult result, ByteBuffer message, SSLEngine engine, SocketChannel channel) throws Exception {
        ByteBuffer aux;

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

    public void processRequests() throws Exception {
        do {
            selector.select();
            Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable())
                    register(key);
                else if (key.isReadable())
                    read((SocketChannel) key.channel(), (SSLEngine) key.attachment());

                iterator.remove();
            }
        } while (true);
    }

    private void register(SelectionKey key) throws Exception {
        SocketChannel channel = ((ServerSocketChannel) key.channel()).accept();
        channel.configureBlocking(false);

        SSLEngine engine = configureSSLEngine();
        handshake(channel, engine);
        channel.register(selector, SelectionKey.OP_READ, engine);
    }

    private SSLEngine configureSSLEngine() throws SSLException {
        SSLEngine engine = context.createSSLEngine();
        engine.setUseClientMode(false);
        engine.beginHandshake();
        return engine;
    }
}