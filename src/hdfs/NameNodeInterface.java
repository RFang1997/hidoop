package hdfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

public interface NameNodeInterface extends Remote{
	
	public void createServerList(String fname, int nombreOrdi) throws RemoteException;
	
	public HashMap<String, ArrayList<String>> getAllProject() throws RemoteException;
	
	public int getPort() throws RemoteException;
	
	public void supprimer(String fname) throws RemoteException;

	public HashMap<Integer, String> getOrdis() throws RemoteException;

}
