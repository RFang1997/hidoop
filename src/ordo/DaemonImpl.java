package ordo;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

import formats.Format;
import map.MapReduce;
import map.Mapper;
import map.Reducer;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String URL;
	private ArrayList<Thread> listethreadsmap;
	private Thread threadreducelocal;

	protected DaemonImpl() throws RemoteException {
		super();
		this.listethreadsmap = new ArrayList<Thread>();
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {	
		//ouverture des formats en lecture et en ecriture
		reader.open(Format.OpenMode.R); 
		writer.open(Format.OpenMode.W);
		
		//application du map sur les formats
		m.map(reader, writer);
			
		//fermeture des formats
		reader.close();
		writer.close();
		
		cb.rendre();
		
	}
	
	@Override
	public void runReduce(Reducer r, Format reader, Format writer, CallBack cb) throws RemoteException {
		//ouverture des formats en lecture et en ecriture
		reader.open(Format.OpenMode.R); 
		writer.open(Format.OpenMode.W);
				
		//application du map sur les formats
		r.reduce(reader, writer);
					
		//fermeture des formats
		reader.close();
		writer.close();
				
		cb.rendre();
		
	}
	
	public String getURL() {
		return this.URL;
	}
	
	public void setURL(String url){
		this.URL = url;
	}
	
	public ArrayList<Thread> getListethreadsmap() {
		return listethreadsmap;
	}
	
	public void setListeThreadsmap(MapReduce mr, Format reader, Format writer, CallBack cb) {
		try {
			Thread thread = new Thread(new ApplyMap(mr,reader,writer,cb));
			this.listethreadsmap.add(thread);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void lancerThreads() {
		for (Thread thread : this.listethreadsmap) {
			thread.start();
		}
	}
	
	public void joinThreads() {
		for (Thread thread : this.listethreadsmap) {
			try {
				thread.join();
				System.out.println("Map termine");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void setReduceLocal(MapReduce mr, Format reader, Format writer, CallBack cb, String fname, int pos, int nombreMaps, int nombreDemons) {
		try {
			this.threadreducelocal = new Thread(new ApplyLocalReduce(mr,reader,writer,cb, fname, pos, nombreMaps, nombreDemons));
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public void lancerReduceLocal() {
		this.threadreducelocal.start();
	}
	
	public void joinReduceLocal() {
		try {
			this.threadreducelocal.join();
			System.out.println("Reduce local termine");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		//Creation d'un demon selon les configurations
		try {
				int port = Integer.parseInt(args[0]);
				DaemonImpl daemonImpl = new DaemonImpl(); //creation d'un Demon
				LocateRegistry.createRegistry(port);
				System.out.println("Creation du demon sur le port " + port + " sur la machine " + InetAddress.getLocalHost().getHostName());
				daemonImpl.setURL("//" + InetAddress.getLocalHost().getHostName() + ":" + String.valueOf(port) + "/DemonRMI"); //creation de l'URL pour enregistrer le demon cree
				System.out.println("Enregistrement du demon avec l'url : " + daemonImpl.getURL());
				Naming.rebind(daemonImpl.getURL(), daemonImpl); //enregistrement
				System.out.println("Demon lance sur la machine " + InetAddress.getLocalHost().getHostName() );
			
		} catch (RemoteException | MalformedURLException | UnknownHostException e) {
				e.printStackTrace();
		}
	}

}
