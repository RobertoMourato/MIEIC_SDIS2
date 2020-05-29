Compilar, coloca o código gerado numa pasta out, correr apartir do root

    javac -d out src/dbs/*.java src/dbs/ssl/*.java
    cp src/dbs/certificates out/dbs -r

Correr:

    cd out

    // 1º Peer
    java dbs.Peer <peerID> <address> <port>

    // Outros peers
    java dbs.Peer <peerID> <address> <port> <existingPeer_address> <existingPeer_port>

    // TestApp
    java dbs.TestApp <address>/<peerID> <protocol> <fileName> <replicationDeg>


Protocols:

    - BACKUP: needs filename and replication degree
    - RESTORE: needs filename
    - DELETE: needs filename (deletes file from all peers)
    - MANAGE: needs filename (deletes file locally)
    - STATE: (no args)