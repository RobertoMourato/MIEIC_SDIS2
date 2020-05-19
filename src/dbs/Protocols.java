package dbs;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Protocols extends Remote {

    String backup(String pathname, int replicationDegree) throws RemoteException;

    String restore(String pathname) throws RemoteException;

    String delete(String pathname) throws RemoteException;

    String manage(String pathName) throws RemoteException;

    String state() throws RemoteException;

}
