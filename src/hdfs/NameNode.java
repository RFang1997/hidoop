package hdfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;

@SuppressWarnings("serial")
public class NameNode extends UnicastRemoteObject implements NameNodeInterface {
	
	private int port;
	private HashMap<Integer, String> ordis = new HashMap<Integer, String>();
	private HashMap<String, ArrayList<String>> allProjects = new HashMap<String, ArrayList<String>>();
	
	//public static final String[] LISTEORDI = {"localhost", "localhost", "localhost"};
	
	public NameNode() throws RemoteException {
		//super();
		this.port = Project.PORTNN;
		for (int i = 0; i < Project.LISTEORDI.length; i++) {
			ordis.put(Project.ports[i], Project.LISTEORDI[i]);
		}
	}
	
	public void createServerList(String fname, int nombreOrdi) throws RemoteException {
		ArrayList<String> serverList = new ArrayList<String>();
		for (int i = 0; i < nombreOrdi; i++) {
			// modulo le nombre de machine disponible pour diviser le travail en 
			// autant de fois que l'on veut
			serverList.add(Project.LISTEORDI[i%ordis.size()]);
			if (i < 3)
				System.out.println(Project.LISTEORDI[i]);
		}
		this.allProjects.put(fname, serverList);
	}
	
	public HashMap<String, ArrayList<String>> getAllProject() {
		return this.allProjects;
	}
	
	public HashMap<Integer, String> getOrdis() {
		return this.ordis;
	}
	
	public int getPort() {
		return this.port;
	}
	
	public void supprimer(String fname) {
		this.allProjects.remove(fname);
	}
	
	public static void main(String args[]) {
		try {
			@SuppressWarnings("unused")
			Registry registry = LocateRegistry.createRegistry(8087);

			//String url = Project.URlNN;
			String url = Project.URlNN;
			Naming.rebind(url, new NameNode());
			System.out.println("NameNode lance");
		} catch (RemoteException | MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
