package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CallBack extends Remote {
	
	public void rendre() throws RemoteException;
	
	public void demander() throws InterruptedException, RemoteException;
	

}
