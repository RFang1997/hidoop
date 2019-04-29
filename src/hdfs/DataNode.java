package hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

import config.Project;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;

public class DataNode extends Thread implements DataNodeInterface {
	
	private Socket s;
	private static HashMap<String, HashMap<Integer, String>> hm =
			new HashMap<String, HashMap<Integer, String>>();
	
	public DataNode (Socket s) {
		this.s = s;
	}
	
	public void receive(String fname, int numero, ObjectInputStream ois) {
		KVFormat fo = new KVFormat();
		fo.setFname(fname);
		fo.setCheminEcriture("/tmp/" + numero + fname);
		fo.open(OpenMode.W);
		
		if (!hm.containsKey(fname))
			hm.put(fname, new HashMap<Integer, String>());
		
		hm.get(fname).put(numero, "/tmp/" + numero + fname);
		
		KV kv;
		try {
			System.out.println("Reception des donnees et ecriture sur le serveur...");
			while ((kv = (KV) ois.readObject()) != null) {
				System.out.println("Reception donnee ok.");
				fo.write(kv);
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (EOFException e) {
			// Sinon exception non voulue de fin de fichier, la machine aime pas trop ca
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fo.close();
	}
	
	public void send(String fname, int numero) {
		KVFormat fo = new KVFormat();
		fo.setFname(fname);
		fo.setCheminEcriture("/tmp/" + numero + fname);
		fo.open(OpenMode.R);
		System.out.println("Ouverture du fichier...");
		try {
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			
			KV kv;
			System.out.println("Ecriture des donnees dans l'Output...");
			while ((kv = fo.read()) != null) {
				oos.writeObject(kv);
				System.out.println("Donnee ecrite ok.");
			}
			
			oos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fo.close();
	}
	
	public void delete(String fname, int numero) {
		System.out.println("Suppression des fichiers...");
		HashMap<Integer, String> hmm = hm.get(fname);
		String cheminFichier = hmm.get(numero);
		File f = new File(cheminFichier);
		if (f.exists()) f.delete();
		else System.out.println("Fichier non existant");
		hm.get(fname).remove(numero);
		System.out.println("Suppression des fichiers terminee.");
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(Project.PORTDN);
			System.out.println("DataNode lance");
			while (true) {
				new Thread(new DataNode(ss.accept())).start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void run() {
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(this.s.getInputStream());
			System.out.println("Reception du message...");
			Message m = (Message) ois.readObject();
			
			if (m.getBut().equals("ecrire"))
				receive(m.getFname(), m.getNumero(), ois);
			else if (m.getBut().equals("lire")) {
				send(m.getFname(), m.getNumero());
			}
			else if (m.getBut().equals("supprimer")) {
				delete(m.getFname(), m.getNumero());
			}
			
			ois.close();
			s.close();
			
		} catch (EOFException e1) {
			// Sinon exception non voulue de fin de fichier, la machine aime pas trop ca
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
