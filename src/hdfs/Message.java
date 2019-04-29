package hdfs;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Message implements Serializable {
	
	private String but;
	private String fname;
	private int numero;
	
	public Message(String but, String fname, int numero) {
		this.but = but;
		this.fname = fname;
		this.numero = numero;
	}
	
	public String getBut() {
		return this.but;
	}
	
	public String getFname() {
		return this.fname;
	}
	
	public int getNumero() {
		return this.numero;
	}

}
