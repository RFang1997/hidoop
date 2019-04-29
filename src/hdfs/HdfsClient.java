/* une PROPOSITION de squelette, incomplete et adaptable... */

package hdfs;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import config.Project;
import formats.Format;
import formats.Format.OpenMode;
import formats.Format.Type;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;

public class HdfsClient {

	private static void usage() {
		System.out.println("Usage: java HdfsClient read <file>");
		System.out.println("Usage: java HdfsClient write <line|kv> <file>");
		System.out.println("Usage: java HdfsClient delete <file>");
	}

	public static void HdfsDelete(String hdfsFname) {
		System.out.println("Recuperation de la liste des serveurs");
		ArrayList<String> serverList = null;
		Socket s = null;
		ObjectOutputStream oos = null;
		try {
			Remote r = Naming.lookup(Project.URlNN);
			if (r instanceof NameNodeInterface) {
				serverList = ((NameNodeInterface) r).getAllProject().get(hdfsFname);
			}
			
			System.out.println("Liste des serveurs recuperee.");

			for (int i =  0; i < serverList.size(); i++) {
				System.out.println("Creation de la socket...");
				s = new Socket(serverList.get(i), Project.PORTDN);

				Message m = new Message("supprimer", hdfsFname, i);
				oos = new ObjectOutputStream(s.getOutputStream());
				oos.writeObject(m);
				System.out.println("Envoi du message...");
			}

			System.out.println("fichiers supprimes des dataNodes, on peut "
					+ "maintenant les supprimer du nameNode");

			((NameNodeInterface) r).supprimer(hdfsFname);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println("Erreur creation socket");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Erreur creation socket ou tout autre erreur " + "d'ailleurs");
		}
		
		try {
			oos.close();
			s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, int repFactor) {

		Format fo;
		if (fmt == Type.KV) {
			fo = new KVFormat();
			fo.setFname(localFSSourceFname);
			fo.setCheminEcriture("/tmp/data/" + localFSSourceFname);
		} else {
			fo = new LineFormat();
			fo.setFname(localFSSourceFname);
			fo.setCheminEcriture("/tmp/data/" + localFSSourceFname);
		}
		
		int tailleFichier = fo.getSize();
		
		fo.open(OpenMode.R);

		// Division du Fichier en Fragment
		int nbFragment = Project.nbFragments;
		int tailleFrag = tailleFichier / nbFragment + 1; /* on rajoute 1 pour etre sur de ne pas oublier
		d'informations */
		
		System.out.println("taille du fichier : " + tailleFichier);
		System.out.println("taille du fragment : " + tailleFrag);

		System.out.println("Creation d'une liste de serveur pour nos fragments...");

		// Creation liste des serveurs pour notre fichier
		ArrayList<String> serverList = null; // pour pouvoir l'utiliser en
												// dehors des
												// clauses try/catch
		try {
			Remote r = Naming.lookup("//localhost:8087/nn");
			if (r instanceof NameNodeInterface) {
				((NameNodeInterface) r).createServerList(localFSSourceFname, nbFragment);
				serverList = ((NameNodeInterface) r).getAllProject().get(localFSSourceFname);
			}

		} catch (RemoteException | MalformedURLException | NotBoundException e1) {
			// TODO Auto-generated catch block
			System.out.println("Probleme lookup");
			e1.printStackTrace();
		}

		System.out.println("Liste des serveurs recuperee.");
		
		Socket sFrag = null;
		ObjectOutputStream oosFrag = null;

		// Pour chaque Fragment, on les envoie
		for (int i = 0; i < nbFragment; i++) {

			// On cree un socket
			try {
				// On cree un socket et notre stream out
				sFrag = new Socket(serverList.get(i), Project.PORTDN);
				oosFrag = new ObjectOutputStream(sFrag.getOutputStream());

				// Pour envoyer un message pour prevenir le serveur DN que l'on
				// va ecrire
				Message message = new Message("ecrire", localFSSourceFname, i);
				System.out.println("Envoi du message...");
				oosFrag.writeObject(message);

				// Et nos donnees
				KV kv;
				int j = 0;
				System.out.println("Envoi des donnees...");
				while (j < tailleFrag && (kv = fo.read()) != null) {
					System.out.println("Envoi donnee ok.");
					oosFrag.writeObject(kv);
					j++;
				}
				System.out.println("");
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		fo.close();
		try {
			oosFrag.close();
			sFrag.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void HdfsRead(String hdfsFname, String localFSDestFname) {

		Format fo = new KVFormat();
		fo.setFname(hdfsFname);
		fo.setCheminEcriture("/tmp/"+"data/" + hdfsFname + "-res");
		fo.open(OpenMode.W);

		System.out.println("Recuperation de la liste d'ordinateurs pour notre fichier : " + hdfsFname);
		
		HashMap<Integer, String> ordiList = null;
		try {
			String url = Project.URlNN;
			System.out.println(url);
			Remote r = Naming.lookup(url);
			if (r instanceof NameNodeInterface) {
				ordiList = ((NameNodeInterface) r).getOrdis();
			}
		} catch (MalformedURLException | RemoteException | NotBoundException e1) {
			// TODO Auto-generated catch block
			System.out.println("Erreur recuperation de la liste des ordinateurs");
			e1.printStackTrace();
		}
		
		System.out.println("Liste des ordinateurs recuperee.");
		
		Socket s = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;

		for (int i = 0; i < ordiList.size(); i++) {

			try {
				System.out.println("\nCreation socket et Output");
				s = new Socket(ordiList.get(Project.ports[i]), Project.PORTDN);
				oos = new ObjectOutputStream(s.getOutputStream());
				
				Message m = new Message("lire", "-resreduce-"+hdfsFname, i);
				oos.writeObject(m);
				System.out.println("Envoi du message...");
				
				System.out.println("Creation Input\n");
				ois = new ObjectInputStream(s.getInputStream());

				KV kv;
				System.out.println("Ecriture des donnees recues...");
				while ((kv = (KV) ois.readObject()) != null) {
					System.out.println("Reception donnee ok.");
					fo.write(kv);
					System.out.println("Ecriture donnee ok.");
				}
				System.out.println("");
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (EOFException e) {
				// Sinon exception non voulue de fin de fichier, la machine aime pas trop ca
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Erreur envoie de la requete ou des donnees");
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		fo.close();
		try {
			ois.close();
			oos.close();
			s.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// java HdfsClient <read|write> <line|kv> <file>

		try {
			if (args.length < 2) {
				usage();
				return;
			}

			switch (args[0]) {
			case "read":
				HdfsRead(args[1], null);
				break;
			case "delete":
				HdfsDelete(args[1]);
				break;
			case "write":
				Format.Type fmt;
				if (args.length < 3) {
					usage();
					return;
				}
				if (args[1].equals("line"))
					fmt = Format.Type.LINE;
				else if (args[1].equals("kv"))
					fmt = Format.Type.KV;
				else {
					usage();
					return;
				}
				HdfsWrite(fmt, args[2], 1);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
