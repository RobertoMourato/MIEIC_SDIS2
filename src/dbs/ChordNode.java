package dbs;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ChordNode {
    static final Integer m = 30;            // Number of bits of each id

    private Peer peer;
    Long id;                        // Node ID
    private int next = 0;                   // FingerTable position to fix
    List<Finger> finger_table;
    Finger predecessor = null;

    ChordNode(Peer peer, Finger node) {
        this.peer = peer;
        this.id = Utils.hash_id_peer(this.peer.host, this.peer.port);
        this.finger_table = new ArrayList<>(m);
        while (finger_table.size() < m) finger_table.add(null);
        join(node);
        this.peer.executor.scheduleAtFixedRate(this::startStabilize, 5, 10, TimeUnit.SECONDS);
        this.peer.executor.scheduleAtFixedRate(this::fixFingers, 3, 5, TimeUnit.SECONDS);
        this.peer.executor.scheduleAtFixedRate(this::checkPredecessorOnline, 12, 10, TimeUnit.SECONDS);
    }

    ChordNode(Peer peer) {
        this.peer = peer;
        this.id = Utils.hash_id_peer(this.peer.host, this.peer.port);
        this.finger_table =  new ArrayList<>(m);
        while (finger_table.size() < m) finger_table.add(null);
        create();
        this.peer.executor.scheduleAtFixedRate(this::startStabilize, 5, 10, TimeUnit.SECONDS);
        this.peer.executor.scheduleAtFixedRate(this::fixFingers, 3, 5, TimeUnit.SECONDS);
    }

    /**
     * Create a new Chord ring
     */
    private void create() {
        this.predecessor = null;
        this.finger_table.set(0, new Finger(this.id, this.peer.host, this.peer.port));
    }

    /**
     * Join a Chord ring containing node n
     * @param n
     */
    private void join(Finger n) {
        this.predecessor = null;
        sendFindSuccessorMessage(this.peer.host, this.peer.port, this.id, this.id, 0, n);
        // Initiate successor -> Peer
    }

    void lookup(Long identifier){
        find_successor(identifier, new Finger(this.id, this.peer.host, this.peer.port), -1);
    }

    void find_successor(Long identifier, Finger askingFinger, int fingerTablePos) {
        Finger successor = finger_table.get(0);
        Long successor_id = successor.getId();

        // The peer is alone, so he is the successor
        if (successor_id.equals(this.id)) {
            if (!askingFinger.getId().equals(this.id))
                finger_table.set(0, askingFinger);
            sendFoundSuccessorMessage(this.peer.host, this.peer.port, this.id, identifier, fingerTablePos, askingFinger);
            return;
        }

        if (successor_id < this.id &&
                (identifier.compareTo(this.id) > 0 || identifier.compareTo(successor_id) <= 0)) {  // interval contains the "begin" and "end" of circle
            sendFoundSuccessorMessage(successor.getHost(), successor.getPort(), successor.getId(), identifier, fingerTablePos, askingFinger);
        } else if (identifier.compareTo(this.id) > 0 && identifier.compareTo(successor_id) <= 0) {
            sendFoundSuccessorMessage(successor.getHost(), successor.getPort(), successor.getId(), identifier, fingerTablePos, askingFinger);
        } else {
            Finger n0 = closest_preceding_node(identifier);

            sendFindSuccessorMessage(askingFinger.getHost(), askingFinger.getPort(), askingFinger.getId(),
                            identifier, fingerTablePos, n0);
        }
    }

    private Finger closest_preceding_node(Long identifier) {
        for (int i = m - 1; i >= 0; i--) {
            if (finger_table.size() > i && finger_table.get(i) != null) {
//                id < finger < identifier
                Finger curFinger = finger_table.get(i);
                if (identifier < this.id &&
                        (curFinger.getId().compareTo(this.id) > 0 || (curFinger.getId().compareTo(identifier) <= 0))) {  // interval contains the "begin" and "end" of circle
                    return curFinger;
                } else if ((curFinger.getId().compareTo(this.id) > 0) && (curFinger.getId().compareTo(identifier) <= 0))
                    return curFinger;

            }
        }

        return new Finger(this.id, this.peer.host, this.peer.port);
    }

    private void startStabilize(){
        sendAskForPredecessorMessage(this.peer.host, this.peer.port, this.id, finger_table.get(0));
    }

    void stabilize(Finger predecessorOfSuccessor) {

        if (predecessorOfSuccessor != null) {
            if (finger_table.get(0).getId() < this.id &&
                    (this.id < predecessorOfSuccessor.getId() || predecessorOfSuccessor.getId() < finger_table.get(0).getId())){ // interval contains the "begin" and "end" of circle
                this.finger_table.set(0, predecessorOfSuccessor);
            } else if (this.id < predecessorOfSuccessor.getId() && predecessorOfSuccessor.getId() < finger_table.get(0).getId()){
                this.finger_table.set(0, predecessorOfSuccessor);
            }
        }
//        if (predecessorOfSuccessor != null && this.id < predecessorOfSuccessor.getId() && predecessorOfSuccessor.getId() < finger_table.get(0).getId())

        sendNotifyMessage(this.peer.host, this.peer.port, this.id, this.finger_table.get(0));
    }

    void notify(Finger possiblePredecessor){
        if (predecessor == null && !possiblePredecessor.getId().equals(this.id)){
            predecessor = possiblePredecessor;
        } else if (this.id < predecessor.getId() &&
                (predecessor.getId() < possiblePredecessor.getId() || possiblePredecessor.getId() < this.id)) { // interval contains the "begin" and "end" of circle
            predecessor = possiblePredecessor;
        } else if (predecessor.getId() < possiblePredecessor.getId() && possiblePredecessor.getId() < this.id){
            predecessor = possiblePredecessor;
        }

        if (predecessor == null || (predecessor.getId() < possiblePredecessor.getId() && possiblePredecessor.getId() < this.id)){
            predecessor = possiblePredecessor;
        }
    }

    private void fixFingers() {
        next = next + 1;
        if(next >= m)
            next = 0;
//        if(next % 10 == 0)
//        System.out.println("TABLE\nTABLE\nTABLE\nTABLE\n");

//        printFingerTable();
        Long requestedId = (this.id + (1L << next)) % (1L << m);
        find_successor(requestedId, new Finger(this.id, this.peer.host, this.peer.port), next);
//        sendFindSuccessorMessage(this.peer.host, this.peer.port, this.id, requestedId, next, closest_preceding_node(requestedId));
    }

    private void checkPredecessorOnline(){

        if (predecessor != null){

            sendAskCheckPredecessorMessage(this.peer.host, this.peer.port, this.id, predecessor);

            try {
                TimeUnit.MILLISECONDS.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (!this.peer.isActivePredecessor){
                if (finger_table.get(0).getId().equals(predecessor.getId())){
                    finger_table.set(0, new Finger(this.id, this.peer.host, this.peer.port));
                }
                predecessor = null;
            }

            this.peer.isActivePredecessor = false;

        }

    }

    private void sendAskCheckPredecessorMessage(String askingHost, Integer askingPort, Long askingId, Finger fingerToSend) {
        StringBuilder message = new StringBuilder();
        message.append("ASK_CHECK_PREDECESSOR").append(" ");
        message.append(askingHost).append(" ");
        message.append(askingPort).append(" ");
        message.append(askingId).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    void sendAnswerForPredecessorMessage(String answerHost, Integer answerPort, Long answerId, Finger fingerToSend) {
        StringBuilder message = new StringBuilder();
        message.append("ANSWER_FOR_PREDECESSOR").append(" ");
        message.append(answerHost).append(" ");
        message.append(answerPort).append(" ");
        message.append(answerId).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendAskForPredecessorMessage(String askingHost, Integer askingPort, Long askingId, Finger fingerToSend) {
        StringBuilder message = new StringBuilder();
        message.append("ASK_FOR_PREDECESSOR").append(" ");
        message.append(askingHost).append(" ");
        message.append(askingPort).append(" ");
        message.append(askingId).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendNotifyMessage(String predecessorHost, Integer predecessorPort, Long predecessorId, Finger fingerToSend) {
        StringBuilder message = new StringBuilder();
        message.append("NOTIFY").append(" ");
        message.append(predecessorHost).append(" ");
        message.append(predecessorPort).append(" ");
        message.append(predecessorId).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendFindSuccessorMessage(String askingHost, Integer askingPort, Long askingId, Long wantedEntity, int fingerTablePos, Finger fingerToSend){
        StringBuilder message = new StringBuilder();
        message.append("FIND_SUCCESSOR").append(" ");
        message.append(askingHost).append(" ");
        message.append(askingPort).append(" ");
        message.append(askingId).append(" ");
        message.append(wantedEntity).append(" ");
        message.append(fingerTablePos).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendFoundSuccessorMessage(String answerHost, Integer answerPort, Long answerId, Long wantedEntity, int fingerTablePos, Finger fingerToSend){
        StringBuilder message = new StringBuilder();
        message.append("FOUND_SUCCESSOR").append(" ");
        message.append(answerHost).append(" ");
        message.append(answerPort).append(" ");
        message.append(answerId).append(" ");
        message.append(wantedEntity).append(" ");
        message.append(fingerTablePos).append(" \r\n");

        try {
            this.peer.chordEngine.sendMessageToPeer(message.toString().getBytes(), fingerToSend);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    String getFingerTableString(){
        StringBuilder table = new StringBuilder();
        table.append("FINGER TABLE FOR PEER ").append(this.id).append(":\n");
        if (predecessor != null){
            table.append("PREDECESSOR").append(": ").append(predecessor.getId()).append(" ").
                    append(predecessor.getHost()).append(" ").
                    append(predecessor.getPort()).append("\n");
        } else {
            table.append("PREDECESSOR").append(": ").append("NOT DEFINED").append("\n");
        }
        for(int i = 0; i < m; i++){
            if (finger_table.size() > i && finger_table.get(i) != null){
                table.append(i).append(": ").append(finger_table.get(i).getId()).append(" ").
                        append(finger_table.get(i).getHost()).append(" ").
                        append(finger_table.get(i).getPort()).append("\n");
            } else {
                table.append(i).append(": ").append("NOT DEFINED").append("\n");
            }
        }
//        System.out.print(table.toString());
        return table.toString();
    }
}
