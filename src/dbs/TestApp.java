package dbs;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class TestApp {
    public static void main(String[] args) {

        if (args.length < 2 || args.length > 4) {
            System.out.println("ERROR: App arguments invalid: TestApp <host>/<peer_id> <sub_protocol> <opnd_1> <opnd_2>");
            return;
        }

        try {
            String[] peerInfo = args[0].split("/");
            String host = peerInfo[0];
            String peerID = peerInfo[1];

            Registry registry = LocateRegistry.getRegistry(host);
            Protocols peer = (Protocols) registry.lookup(peerID);

            String filePath;
            int replicationDegree;

            switch (args[1]) {
                case "BACKUP":

                    if (args.length != 4) {
                        System.out.println("ERROR: Backup arguments invalid: BACKUP <file_path> <replication_degree>");
                        return;
                    }

                    filePath = args[2];
                    replicationDegree = Integer.parseInt(args[3]);
                    System.out.println(peer.backup(filePath, replicationDegree));
                    break;
                case "RESTORE":

                    if (args.length != 3) {
                        System.out.println("ERROR: Restore arguments invalid: RESTORE <file_path>");
                        return;
                    }

                    filePath = args[2];
                    System.out.println(peer.restore(filePath));
                    break;
                case "DELETE":

                    if (args.length != 3) {
                        System.out.println("ERROR: Delete arguments invalid: DELETE <file_path>");
                        return;
                    }

                    filePath = args[2];
                    System.out.println(peer.delete(filePath));
                    break;
                case "MANAGE":

                    if (args.length != 3) {
                        System.out.println("ERROR: Reclaim arguments invalid: MANAGE <file_path>");
                        return;
                    }

                    filePath = args[2];
                    System.out.println(peer.manage(filePath));

                    break;
                case "STATE":

                    if (args.length != 2) {
                        System.out.println("ERROR: State format must be: STATE");
                        return;
                    }

                    System.out.println(peer.state());
                    break;
            }

        } catch (Exception e) {
            System.err.println("Application exception: " + e.toString());
            e.printStackTrace();
        }

    }
}
