package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected StatusTache status;
	protected Semaphore semaphore;
	
	 public CallBackImpl() throws RemoteException {
		 this.semaphore = new Semaphore(0);
		 this.status = StatusTache.EN_COURS;      
	  }
	  
	 public void rendre() throws RemoteException {
		 	this.semaphore.release();
	        this.status = StatusTache.FINIE;
	  }

	 public void demander() throws RemoteException, InterruptedException {
	            this.semaphore.acquire();
	            this.status = StatusTache.EN_DEMANDE;
	  }

	
}
