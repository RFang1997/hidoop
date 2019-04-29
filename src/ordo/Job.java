package ordo;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.NameNodeInterface;
import map.MapReduce;

public class Job implements JobInterface {
	protected Type inputFormat;
	protected String inputFname;
	protected int numberofmaps;
	protected int numberofdemons;
	protected Format reader;
	protected Format writer;
	private HashMap<Integer, String> ordis;
	
	@Override
	public  void  setInputFormat(Format.Type format) {
		this.inputFormat = format;
	}
	
	@Override
	public  void  setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberofmaps = tasks;
	}

	public void setNumberOfDemons(int nombreDemons) {
		this.numberofdemons = nombreDemons;
	}

	public  void  startJob (MapReduce mr) {
		//Creation du Callback pour les maps
		CallBack cb_m = null;
		try {
			cb_m = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		
		//Recuperation de la liste des informations du NameNode
		ArrayList<String> serverList = null;
		try {
			System.out.println("Connection au NameNode d'URL : " + "//localhost:8087/nn");
			Remote r = Naming.lookup("//localhost:8087/nn");
			if (r instanceof NameNodeInterface) {
				serverList = ((NameNodeInterface) r).getAllProject().get(this.inputFname);
			}
			this.ordis = ((NameNodeInterface) r).getOrdis();
		} catch (RemoteException | MalformedURLException | NotBoundException e1) {
			// TODO Auto-generated catch block
			System.out.println("Problème lookup");
			e1.printStackTrace();
		}
		
		//Mise en place du nombre de maps a effectuer
		setNumberOfMaps(serverList.size());
		
		//Mise en place du nombre de demons lances
		setNumberOfDemons(this.ordis.size());
		
		Map<String, Integer> HashMapReverse = new HashMap<>();
		for(Map.Entry<Integer, String> entry : this.ordis.entrySet()){
			HashMapReverse.put(entry.getValue(), entry.getKey());
		}
		HashMap<String, Integer> reverseOrdis = (HashMap<String, Integer>) HashMapReverse;
		
		Daemon[] listedemons = new Daemon[this.ordis.size()];
		
		//Connection aux differents demons et mise en place des maps sur chaque demon
		try {
			int i = 0;
			for(String server : serverList) {
						Integer port = reverseOrdis.get(server);
						//initialisation du Demon courant
						String URL = "//" + server + ":" + port + "/DemonRMI";
						System.out.println("Connection au demon d'URL : " + URL);
						Daemon r = (Daemon) Naming.lookup(URL);
					
						//Creation des fichiers
						if (this.inputFormat == Type.LINE) {
							this.reader = new LineFormat();
							this.reader.setFname(i+this.inputFname);
							this.reader.setCheminEcriture("/tmp/"+this.reader.getFname());
							this.writer = new KVFormat();
							this.writer.setFname(i+"-resmap-"+this.inputFname);
							this.writer.setCheminEcriture("/tmp/"+this.writer.getFname());
						} else {
							this.reader = new KVFormat();
							this.reader.setFname(i+this.inputFname);
							this.reader.setCheminEcriture("/tmp/"+this.reader.getFname());
							this.writer = new KVFormat();
							this.writer.setFname(i+"-resmap-"+this.inputFname);
							this.writer.setCheminEcriture("/tmp/"+this.writer.getFname());
						}
						
						//Creation d'un thread pour le demon courant
						System.out.println("Creation du thread numero "+i+" sur le demon d'url : "+URL);
						r.setListeThreadsmap(mr,reader,writer,cb_m);
						if (i< this.numberofdemons) {
							listedemons[i] = r;
						}
						i += 1;			
				}
			
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
				e.printStackTrace();
		}
		
		//Lancements des maps sur les differents demons
		for (int i=0; i<numberofdemons;i++) {
			try {
				listedemons[i].lancerThreads();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	
		//Attente de la terminaison de tous les maps
		for (int i=0; i<numberofdemons;i++) {
			try {
				listedemons[i].joinThreads();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// attente du CallBack des maps
		for (int i=0; i<numberofdemons;i++) {
			try {
				cb_m.demander();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//Creation du Callback pour les reduces locaux
		CallBack cb_rl = null;
		try {
			cb_rl = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
		
		//Mise en place du reduce local sur chaque demon
		for (int i=0; i<numberofdemons;i++) {
			try {
				Daemon demon = listedemons[i];
				
				//Creation du fichier reduce
				this.reader = new KVFormat();
				this.reader.setFname(i+"-resfusionmap-"+this.inputFname);
				this.reader.setCheminEcriture("/tmp/" +this.reader.getFname());
				this.writer = new KVFormat();
				this.writer.setFname(i+"-resreduce-"+this.inputFname);
				this.writer.setCheminEcriture("/tmp/"+this.writer.getFname());
				
				demon.setReduceLocal(mr, this.reader, this.writer, cb_rl, this.inputFname, i, this.numberofmaps, this.numberofdemons);
		
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		//Lancements du reduce local sur les differents demons
		for (int i=0; i<numberofdemons;i++) {
			try {
				listedemons[i].lancerReduceLocal();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		
		//Attente de la terminaison de tous les reduce locaux
		for (int i=0; i<numberofdemons;i++) {
			try {
				listedemons[i].joinReduceLocal();
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// attente du CallBack des reduces locaux
		for (int i=0; i<numberofdemons;i++) {
			try {
				cb_rl.demander();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//Rassemblage des fragments localement reduce sur la machine mere
		HdfsClient.HdfsRead(this.inputFname, null);
		
		//Creation du Callback pour le reduce global
		CallBack cb_r = null;
		try {
			cb_r = new CallBackImpl();
		} catch (RemoteException e1) {
			e1.printStackTrace();
		}
	
		//Reduce final		
		//Creation du fichier reduce
		this.reader = new KVFormat();
		this.reader.setFname(this.inputFname+"-res");
		this.reader.setCheminEcriture("/tmp/" +"data/"+this.reader.getFname());
		this.writer = new KVFormat();
		this.writer.setFname("resfinal-"+this.inputFname);
		this.writer.setCheminEcriture("/tmp/" +"data/"+this.writer.getFname());
			
		System.out.println("Debut du reduce final...");
		this.reader.open(Format.OpenMode.R); 
		this.writer.open(Format.OpenMode.W);
				
		//application du map sur les formats
		mr.reduce(reader, writer);
					
		//fermeture des formats
		reader.close();
		writer.close();
				
		try {
			cb_r.rendre();
		} catch (RemoteException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Reduce final termine");
				
		try {
			cb_r.demander();
		} catch (RemoteException | InterruptedException e) {
			e.printStackTrace();
		}
	}

}
