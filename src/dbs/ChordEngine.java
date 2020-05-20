package dbs;

import dbs.ssl.Client;
import dbs.ssl.Server;

public class ChordEngine {
    private Peer peer;

    private Server server;

    public ChordEngine(Peer peer) {
        this.peer = peer;

        try {
            server = new Server(this.peer, this.peer.host, this.peer.port);
            this.peer.executor.execute(() -> {
                try {
                    server.processRequests();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void sendMessageToPeer(byte[] message, Finger peerToSend) {
        this.peer.executor.execute(() -> {
            try {
                Client client1 = new Client(peerToSend.host, peerToSend.port);
                client1.connect();
                client1.write(message);
                client1.shutdown();
            } catch (Exception e) {
                System.out.println("COULDN'T CONNECT TO PEER " + peerToSend.port);
            }
        });
    }
}
