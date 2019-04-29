package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class KVFormat implements Format {
	
	private static final long serialVersionUID = 1L;
	private OpenMode om;
	private File f;
	private FileReader fr;
	private FileWriter fw;
	private BufferedReader br;
	private BufferedWriter bw;
	private Integer index;
	private String fname;
	private String cheminEcriture;

	@Override
	public KV read() {
		// TODO Auto-generated method stub
		try {
			String l = this.br.readLine();
			if (l == "") {
				/* alors la ligne est vide, on renvoie la conversion de l'index
				et la chaine vide */
				System.out.println("ligne vide");
				return new KV(String.valueOf(index), "");
			} else if (l == null) {
				return null;
			} else {
				/* Sinon alors on split en separant les deux parties */
				String[] texte = l.split("<->");
				return new KV(texte[0], texte[1]);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Erreur bufferReader");
			return null;
		}
	}

	@Override
	public void write(KV record) {
		// TODO Auto-generated method stub
		try {
			this.fw.write(record.k + "<->" + record.v + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void open(OpenMode mode) {
		// TODO Auto-generated method stub
		this.om = mode;
		if (this.om == Format.OpenMode.R) {
			try {
				//this.f = new File("data/" + this.fname);
				this.f = new File(this.cheminEcriture);
				this.fr = new FileReader(this.f);
				this.br = new BufferedReader(this.fr);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				//this.f = new File("data/" + this.fname);
				this.f = new File(this.cheminEcriture);
				this.fw = new FileWriter(this.f);
				this.bw = new BufferedWriter(this.fw);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		if (this.om == Format.OpenMode.R) {
			try {
				this.fr.close();
				this.br.close();
				this.index = 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				this.fw.close();
				this.bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public long getIndex() {
		// TODO Auto-generated method stub
		return this.index;
	}

	@Override
	public String getFname() {
		// TODO Auto-generated method stub
		return this.fname;
	}

	@Override
	public void setFname(String fname) {
		this.fname = fname;
	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		int i = 0;
		this.open(OpenMode.R);
		while (this.read() != null) {
			i++;
		}
		this.close();
		return i;
	}
	
	public void setCheminEcriture(String chemin) {
		this.cheminEcriture = chemin;
	}
	
	public String getCheminEcriture() {
		return this.cheminEcriture;
	}

}
