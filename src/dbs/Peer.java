package dbs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Peer implements Protocols {
    private String id;
    String host;
    Integer port;
    private ChordNode chordNode;
    ChordEngine chordEngine;
    public ScheduledThreadPoolExecutor executor;

    private ConcurrentHashMap<Long, Finger> fileToPeer = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, Boolean> haveFile = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, Boolean> isReceivingFile = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, Long> filesStoredSize = new ConcurrentHashMap<>();

    boolean isActivePredecessor = false;
    boolean isActiveSuccessor = false;

    public Peer(String id, String host, Integer port) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        this.executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(60);
        this.chordEngine = new ChordEngine(this);
        this.chordNode = new ChordNode(this);

        Protocols sender = (Protocols) UnicastRemoteObject.exportObject(this, 0);

        Registry registry = LocateRegistry.getRegistry();
        registry.rebind(this.id, sender);
    }

    public Peer(String id, String host, Integer port, Finger fingerKnown) throws Exception {
        this.id = id;
        this.host = host;
        this.port = port;
        this.executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(10);
        this.chordEngine = new ChordEngine(this);
        this.chordNode = new ChordNode(this, fingerKnown);

        Protocols sender = (Protocols) UnicastRemoteObject.exportObject(this, 0);

        Registry registry = LocateRegistry.getRegistry();

        registry.rebind(this.id, sender);
    }

    private static String getId(String[] args) {
        return args[0];
    }

    private static String getHost(String[] args) {
        return args[1];
    }

    private static Integer getPort(String[] args) {
        return Integer.parseInt(args[2]);
    }

    private static String getChordNodeHost(String[] args) {
        return args[3];
    }

    private static Integer getChordNodePort(String[] args) {
        return Integer.parseInt(args[4]);
    }

    @Override
    public String backup(String pathname, int replicationDegree) {
        File file = new File(pathname);

        if (!file.exists())
            return "FAILED: file not found";

        List<Long> idsOfFile = Utils.hashes_of_file(file, 9);
        List<Integer> orderPositions = Utils.positionsForReplicationDegree(replicationDegree);
        for (int i = 0; i < 9; i++){
            if (!orderPositions.contains(i))
                orderPositions.add(i);
        }

        if (idsOfFile.size() == 0)
            return "FAILED: fileId couldn't be generated";

        for (int i = 0; i < idsOfFile.size(); i++){
            this.chordNode.lookup(idsOfFile.get(i));

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int goodRep = 0;

        Set<Long> usedPeers = new HashSet<>();

        for (int i = 0; i < orderPositions.size(); i++){

            if (fileToPeer.get(idsOfFile.get(orderPositions.get(i))) == null){
                System.out.println("FAILED: peer to store file not found");
            } else {
                Finger fingerToStore = fileToPeer.get((idsOfFile.get(orderPositions.get(i))));
                fileToPeer.remove((idsOfFile.get(orderPositions.get(i))));

                if (!usedPeers.contains(fingerToStore.getId()) && goodRep < replicationDegree){
                    usedPeers.add(fingerToStore.getId());
                    sendPutChunk(file, idsOfFile.get(orderPositions.get(i)), fingerToStore);
                    System.out.println("GOOD " + fingerToStore.getPort());
                    goodRep++;
                } else {
                    System.out.println("BAD " + fingerToStore.getPort() + "  " + orderPositions.size() + " " + goodRep);
                }

            }
        }

        return "SENT " + goodRep + "/" + replicationDegree + " REPLICAS";
    }

    @Override
    public String restore(String pathname) {
        File file = new File(pathname);

        if (!file.exists())
            return "FAILED: file not found";

        List<Long> idsOfFile = Utils.hashes_of_file(file, 9);

        if (idsOfFile.size() == 0)
            return "FAILED: fileId couldn't be generated";

        for (int i = 0; i < idsOfFile.size(); i++){
            this.chordNode.lookup(idsOfFile.get(i));

            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (fileToPeer.get(idsOfFile.get(i)) == null){
                System.out.println("FAILED: peer with stored file not found");
            } else {
                Finger fingerPossibleWithFile = fileToPeer.get(idsOfFile.get(i));
                fileToPeer.remove(idsOfFile.get(i));

                sendGetChunk(idsOfFile.get(i), pathname, fingerPossibleWithFile);

                try {
                    TimeUnit.MILLISECONDS.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (isReceivingFile.get(idsOfFile.get(i)) != null){

                    isReceivingFile.remove(idsOfFile.get(i));

                    return "GOOD: IS RECEIVING FILE";
                }


            }

        }

        return "NOT GOOD: COULDN'T GET FILE";

    }

    @Override
    public String delete(String pathname) {
        File file = new File(pathname);

        if (!file.exists())
            return "FAILED: file not found";

        List<Long> idsOfFile = Utils.hashes_of_file(file, 9);
        List<Integer> ports = new ArrayList<>();

        if (idsOfFile.size() == 0)
            return "FAILED: fileId couldn't be generated";

        for (Long fileId : idsOfFile) {
            this.chordNode.lookup(fileId);

            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // This peer doesn't have any file with this id
            if(fileToPeer.get(fileId) == null)
                continue;

            Finger finger = fileToPeer.get(fileId);
            fileToPeer.remove(fileId);

            sendDeleteFile(fileId, finger);
            ports.add(finger.getPort());
        }

        return "GOOD " + ports.toString();
    }

    @Override
    public String manage(String pathName) {
        File file = new File(pathName);

        if (!file.exists())
            return "FAILED: file not found";

        List<Long> idsOfFile = Utils.hashes_of_file(file, 9);

        if (idsOfFile.size() == 0)
            return "FAILED: fileId couldn't be generated";

        for (Long fileId : idsOfFile) {
            String filePath = this.id + "/" + fileId;
            File tmp = new File(filePath);
            if (tmp.delete()) {
                filesStoredSize.remove(fileId);
                System.out.println("Deleted file with ID=" + fileId);
            }
        }
        return "Finished ";
    }

    @Override
    public String state() {
        Long usedSpace = getTotalUsedSpace();
        String listFilesSize = getFilesUsedSpaceList();
        String fingerTable = this.chordNode.getFingerTableString();

        return "Peer using " + usedSpace + " bytes of storage\n" +
                listFilesSize + "\n" +
                fingerTable;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 5) {
            System.out.println("Usage: dbs.Peer <id> <host> <port>");
            System.exit(1);
        }

        String id = getId(args);
        String host = getHost(args);
        Integer port = getPort(args);

        if (args.length == 3){
            Peer peer = new Peer(id, host, port);
        } else {
            String chordNodeHost = getChordNodeHost(args);
            Integer chordNodePort = getChordNodePort(args);
            Long chordNodeId = Utils.hash_id_peer(chordNodeHost, chordNodePort);

            Peer peer = new Peer(id, host, port, new Finger(chordNodeId, chordNodeHost, chordNodePort));
        }
    }

    private Long getTotalUsedSpace(){
        Long sizeOnDisk = 0L;
        Iterator<Map.Entry<Long, Long>> it = filesStoredSize.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> pair = it.next();
            sizeOnDisk += pair.getValue();
        }
        return  sizeOnDisk;
    }

    private String getFilesUsedSpaceList(){
        StringBuilder list = new StringBuilder();
        list.append("List of stored files:\n");

        for(Map.Entry<Long, Long> entry : filesStoredSize.entrySet()) {
            list.append("\t").append(entry.getKey()).append(": ").append(entry.getValue()).append(" bytes\n");
        }

        return list.toString();
    }


    // Não sei se faz sentido o tratamento das mensagens do chord estarem no peer
    public void handleMessage(byte[] message) {

        String mes = new String(message);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


//        String[] arguments = message.split(" ");

        if (arguments.length < 3){
            System.err.println("Invalid received message");
            System.err.println("\"" + new String(message) + "\"");
            return;
        }

        System.out.println("RECEIVED OPERATION: " + arguments[0]);

        switch (arguments[0]) {
            case "FIND_SUCCESSOR":
                System.out.println("FIND_SUCCESSOR " + this.id);
                handleFindSuccessor(message);
                break;
            case "FOUND_SUCCESSOR":
                System.out.println("FOUND_SUCCESSOR " + this.id);
                handleFoundSuccessor(message);
                break;
            case "NOTIFY":
                System.out.println("NOTIFY " + this.id);
                handleNotify(message);
                break;
            case "ASK_FOR_PREDECESSOR":
                System.out.println("ASK_FOR_PREDECESSOR " + this.id);
                handleAskForPredecessor(message);
                break;
            case "ANSWER_FOR_PREDECESSOR":
                System.out.println("ANSWER_FOR_PREDECESSOR " + this.id);
                handleAnswerForPredecessor(message);
                break;
            case "ASK_FOR_FILES":
                System.out.println("ASK FOR FILES " + this.id);
                handleAskForFiles(message);
                break;
            case "PUT_CHUNK":
                System.out.println("PUT_CHUNK " + this.id);
                handlePutChunk(message);
                break;
            case "GET_CHUNK":
                System.out.println("GET_CHUNK " + this.id);
                handleGetChunk(message);
                break;
            case "CHUNK":
                System.out.println("CHUNK " + this.id);
                handleChunk(message);
                break;
            case "DELETE":
                System.out.println("DELETE " + this.id);
                handleDelete(message);
                break;
            case "ASK_CHECK_PREDECESSOR":
                System.out.println("ASK_CHECK_PREDECESSOR " + this.id);
                handleAskCheckPredecessor(message);
                break;
            case "ANSWER_CHECK_PREDECESSOR":
                System.out.println("ANSWER_CHECK_PREDECESSOR " + this.id);
                handleAnswerCheckPredecessor(message);
                break;
            case "ASK_CHECK_SUCCESSOR":
                System.out.println("ASK_CHECK_SUCCESSOR " + this.id);
                handleAskCheckSuccessor(message);
                break;
            case "ANSWER_CHECK_SUCCESSOR":
                System.out.println("ANSWER_CHECK_SUCCESSOR " + this.id);
                handleAnswerCheckSuccessor(message);
                break;
            default:
                System.out.println("ERROR");
                System.out.println("\"" + mes + "\"");
                break;
        }

    }
    private void handleAnswerCheckSuccessor(byte[] message) {
        isActiveSuccessor = true;
    }

    private void handleAskCheckSuccessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);

        Finger askingFinger = new Finger(askingId, askingHost, askingPort);

        sendAnswerCheckSuccessor(this.host, this.port, this.chordNode.id, askingFinger);
    }

    private void sendAnswerCheckSuccessor(String answerHost, Integer answerPort, Long answerId, Finger fingerToSend){

        StringBuilder message = new StringBuilder();
        message.append("ANSWER_CHECK_SUCCESSOR").append(" ");
        message.append(answerHost).append(" ");
        message.append(answerPort).append(" ");
        message.append(answerId).append(" \r\n");

        try {
            this.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleAnswerCheckPredecessor(byte[] message) {
        isActivePredecessor = true;
    }

    private void handleDelete(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");

        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);
        Long fileId = Long.parseLong(arguments[4]);

        String filePath = this.id + "/" + fileId;
        File tmp = new File(filePath);
        tmp.getParentFile().mkdirs();

        System.out.println("RECEIVING DELETE " + filePath);

        tmp.delete();
        filesStoredSize.remove(fileId);
/*
        StringBuilder messageToSend = new StringBuilder();
        messageToSend.append("DELETED").append(" ");
        messageToSend.append(this.host).append(" ");
        messageToSend.append(this.port).append(" ");
        messageToSend.append(this.chordNode.id).append(" ");
        messageToSend.append(fileId).append(" \r\n");

        byte[] mesToSend = messageToSend.toString().getBytes();

        this.chordEngine.sendMessageToPeer(mesToSend, mesToSend);
        */
    }

    private void handleChunk(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");

        String answerHost = arguments[1];
        Integer answerPort = Integer.parseInt(arguments[2]);
        Long answerId = Long.parseLong(arguments[3]);
        int chunkNo = Integer.parseInt(arguments[4]);
        Long fileId = Long.parseLong(arguments[5]);
        String fileName = arguments[6];


        int endHeader = 1;
        while (message[endHeader - 1] != 0xD || message[endHeader] != 0xA)
            endHeader++;

        endHeader++;

        String filePath = id + "/restored/" + fileName;
        File tmp = new File(filePath);
        tmp.getParentFile().mkdirs();

        System.out.println("RECEIVING CHUNK " + chunkNo + "  SIZE:" + message.length);

        try {
            tmp.createNewFile();
            FileOutputStream writeToFile;
            if (chunkNo == 0){
                writeToFile = new FileOutputStream(filePath);
                isReceivingFile.put(fileId, true);
            } else {
                writeToFile = new FileOutputStream(filePath, true);
            }
            writeToFile.write(Arrays.copyOfRange(message, endHeader, message.length));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void handleGetChunk(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");

        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);
        Long fileId = Long.parseLong(arguments[4]);
        String fileName = arguments[5];

        Finger fingerToSend = new Finger(askingId, askingHost, askingPort);

        // TODO  use hashmap

        String filePath = id + "/" + fileId;
        File file = new File(filePath);

        if (!file.exists()){
            System.out.println("ERROR: FILE NOT STORED");
            return;
        }

        byte[] body = new byte[0];

        try {
            body = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        int BYTES_PER_PACKET = 10000;

        for (int i = 0; i*BYTES_PER_PACKET < body.length; i++){

            StringBuilder curMessage = new StringBuilder();
            curMessage.append("CHUNK").append(" ");
            curMessage.append(this.host).append(" ");
            curMessage.append(this.port).append(" ");
            curMessage.append(this.chordNode.id).append(" ");
            curMessage.append(i).append(" ");
            curMessage.append(fileId).append(" ");
            curMessage.append(fileName).append(" \r\n");

            int bodyLength = (int) Math.min(BYTES_PER_PACKET, file.length()-i*BYTES_PER_PACKET); //TODO
            byte[] mesBytes = new byte[bodyLength + curMessage.toString().getBytes().length];
            System.arraycopy(curMessage.toString().getBytes(), 0,
                    mesBytes, 0, curMessage.toString().getBytes().length);
            System.arraycopy(body, i*BYTES_PER_PACKET, mesBytes, curMessage.toString().getBytes().length, bodyLength);

            System.out.println("SENDING CHUNK " + i);

            this.chordEngine.sendMessageToPeer(mesBytes, fingerToSend);

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void sendAnswerCheckPredecessor(String answerHost, Integer answerPort, Long answerId, Finger fingerToSend){

        StringBuilder message = new StringBuilder();
        message.append("ANSWER_CHECK_PREDECESSOR").append(" ");
        message.append(answerHost).append(" ");
        message.append(answerPort).append(" ");
        message.append(answerId).append(" \r\n");

        try {
            this.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleAskCheckPredecessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String answerHost = arguments[1];
        Integer answerPort = Integer.parseInt(arguments[2]);
        Long answerId = Long.parseLong(arguments[3]);

        Finger askingFinger = new Finger(answerId, answerHost, answerPort);

        sendAnswerCheckPredecessor(answerHost, answerPort, answerId, askingFinger);
    }

    private void sendDeleteFile(Long fileId, Finger fingerPossibleWithFile) {

        StringBuilder message = new StringBuilder();
        message.append("DELETE").append(" ");
        message.append(this.host).append(" ");
        message.append(this.port).append(" ");
        message.append(this.chordNode.id).append(" ");
        message.append(fileId).append(" \r\n");

        try {
            this.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerPossibleWithFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendGetChunk(Long fileId, String pathName, Finger fingerPossibleWithFile) {

        StringBuilder message = new StringBuilder();
        message.append("GET_CHUNK").append(" ");
        message.append(this.host).append(" ");
        message.append(this.port).append(" ");
        message.append(this.chordNode.id).append(" ");
        message.append(fileId).append(" ");
        message.append(pathName).append(" \r\n");

        try {
            this.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerPossibleWithFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendPutChunk(File file, Long fileId, Finger fingerToSend){

        byte[] body = new byte[0];

        try {
            body = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            e.printStackTrace();
        }

        int BYTES_PER_PACKET = 10000;

        for (int i = 0; i*BYTES_PER_PACKET < body.length; i++){

            StringBuilder message = new StringBuilder();
            message.append("PUT_CHUNK").append(" ");
            message.append(this.host).append(" ");
            message.append(this.port).append(" ");
            message.append(this.chordNode.id).append(" ");
            message.append(i).append(" ");
            message.append(fileId).append(" \r\n");

            int bodyLength = (int) Math.min(BYTES_PER_PACKET, file.length()-i*BYTES_PER_PACKET); //TODO
            byte[] mes = new byte[bodyLength + message.toString().getBytes().length];
            System.arraycopy(message.toString().getBytes(), 0,
                    mes, 0, message.toString().getBytes().length);
            System.arraycopy(body, i*BYTES_PER_PACKET, mes, message.toString().getBytes().length, bodyLength);

            System.out.println("SENDING PUT_CHUNK " + i);

            this.chordEngine.sendMessageToPeer(mes, fingerToSend);

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void handlePutChunk(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");

        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);
        int chunkNo = Integer.parseInt(arguments[4]);
        Long fileId = Long.parseLong(arguments[5]);

        int endHeader = 1;
        while (message[endHeader - 1] != 0xD || message[endHeader] != 0xA)
            endHeader++;

        endHeader++;

        String filePath = id + "/" + fileId;
        File tmp = new File(filePath);
        tmp.getParentFile().mkdirs();

        System.out.println("RECEIVING PUT_CHUNK " + chunkNo + "  SIZE:" + message.length);

        try {
            tmp.createNewFile();
            FileOutputStream writeToFile;
            if (chunkNo == 0){
                writeToFile = new FileOutputStream(filePath);
                filesStoredSize.put(fileId, (long) (message.length-endHeader));
            } else {
                writeToFile = new FileOutputStream(filePath, true);
                filesStoredSize.put(fileId, filesStoredSize.get(fileId)+message.length-endHeader);
            }
            writeToFile.write(Arrays.copyOfRange(message, endHeader, message.length));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleAnswerForPredecessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String answerHost = arguments[1];
        Integer answerPort = Integer.parseInt(arguments[2]);
        Long answerId = Long.parseLong(arguments[3]);

        Finger answerFinger;

        if (answerHost.equals("null") && answerPort == -1 && answerId == -1)
            answerFinger = null;
        else
            answerFinger = new Finger(answerId, answerHost, answerPort);

        this.chordNode.stabilize(answerFinger);
    }

    private void handleAskForPredecessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);

        Finger askingFinger = new Finger(askingId, askingHost, askingPort);

        if (this.chordNode.predecessor == null)
            this.chordNode.sendAnswerForPredecessorMessage("null", -1, -1L, askingFinger);
        else
            this.chordNode.sendAnswerForPredecessorMessage(this.chordNode.predecessor.getHost(),
                    this.chordNode.predecessor.getPort(), this.chordNode.predecessor.getId(), askingFinger);
    }

    private void handleNotify(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String predecessorHost = arguments[1];
        Integer predecessorPort = Integer.parseInt(arguments[2]);
        Long predecessorId = Long.parseLong(arguments[3]);

        Finger predecessorFinger = new Finger(predecessorId, predecessorHost, predecessorPort);

        this.chordNode.notify(predecessorFinger);
    }

    private void handleAskForFiles(byte[] message) {
        String msg = new String(message, StandardCharsets.US_ASCII);
        msg = msg.trim();
        String[] args = msg.split(" ");

        String askingHost = args[1];
        Integer askingPort = Integer.valueOf(args[2]);
        Long nodeId = Long.valueOf(args[3]);

        Finger finger = new Finger(nodeId, askingHost, askingPort);

        Iterator<Map.Entry<Long, Long>> it = filesStoredSize.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> pair = it.next();
            Long fileId = pair.getKey();
            if ((this.chordNode.id < nodeId && (fileId <= nodeId && fileId > this.chordNode.id)) ||
                    (this.chordNode.id >= nodeId && (fileId <= nodeId || fileId > this.chordNode.id))){
                String filePath = this.id + "/" + fileId;
                File file = new File(filePath);
                if (file.exists()){
                    sendPutChunk(file, fileId, finger);
                    file.delete();
                    it.remove();
                }
            }
        }

//        for(Map.Entry<Long, Long> entry : filesStoredSize.entrySet()) {
//            Long fileId = entry.getKey();
//            if ((this.chordNode.id < nodeId && (fileId <= nodeId && fileId > this.chordNode.id)) ||
//                    (this.chordNode.id >= nodeId && (fileId <= nodeId || fileId > this.chordNode.id))){
//                String filePath = this.id + "/" + fileId;
//                File file = new File(filePath);
//                if (file.exists()){
//                    sendPutChunk(file, fileId, finger);
//                    file.delete();
//                    filesStoredSize.remove(fileId);
//                }
//            }
//        }
    }

    private void handleFoundSuccessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String answerHost = arguments[1];
        Integer answerPort = Integer.parseInt(arguments[2]);
        Long answerId = Long.parseLong(arguments[3]);
        Long questionId = Long.parseLong(arguments[4]);
        int fingerTablePos = Integer.parseInt(arguments[5]);

        Finger answerFinger = new Finger(answerId, answerHost, answerPort);

        if (fingerTablePos >= 0) {
            if (this.chordNode.finger_table.get(0) == null){
                sendMessageAskingForFiles(this.host, this.port, this.chordNode.id, answerFinger);
            }
            this.chordNode.finger_table.set(fingerTablePos, answerFinger);
        }
        else if (fingerTablePos == -2020) {
            this.fileToPeer.put(questionId, answerFinger);
        }
        else {
            synchronized(this.chordNode.successors){
                this.chordNode.successors.set(-fingerTablePos, answerFinger);
            }
        }
    }

    private void sendMessageAskingForFiles(String askingHost, Integer askingPort, Long askingId, Finger fingerToSend) {
        StringBuilder message = new StringBuilder();
        message.append("ASK_FOR_FILES").append(" ");
        message.append(askingHost).append(" ");
        message.append(askingPort).append(" ");
        message.append(askingId).append(" \r\n");

        try {
            this.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleFindSuccessor(byte[] message) {

        String mes = new String(message, StandardCharsets.US_ASCII);
        mes = mes.trim();
        String[] arguments = mes.split(" ");


        String askingHost = arguments[1];
        Integer askingPort = Integer.parseInt(arguments[2]);
        Long askingId = Long.parseLong(arguments[3]);
        Long questionId = Long.parseLong(arguments[4]);
        int fingerTablePos = Integer.parseInt(arguments[5]);

        Finger askingFinger = new Finger(askingId, askingHost, askingPort);

        System.out.println("RUNNING find_successor " + askingHost + " " + askingPort + " " + askingId + " " + questionId);

        this.chordNode.find_successor(questionId, askingFinger, fingerTablePos);
    }
}
