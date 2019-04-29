package ordo;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;

import formats.Format;
import map.MapReduce;

public class ApplyLocalReduce extends DaemonImpl implements Runnable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	MapReduce mr;
	Format reader;
	Format writer;
	CallBack cb;
	String fname;
	int pos;
	int nombreMaps;
	int nombreDemons;
	
	public ApplyLocalReduce(MapReduce mr, Format reader, Format writer, CallBack cb, String fname, int pos, int nombreMaps, int nombreDemons) throws RemoteException {
		this.mr = mr;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
		this.fname = fname;
		this.pos = pos;
		this.nombreMaps = nombreMaps;
		this.nombreDemons = nombreDemons;
	}
	
	public void run() {
		try {
			System.out.println("Application du reduce local en cours ...");
			InputStream input;
			FileOutputStream output;
			
			try {
				output = new FileOutputStream("/tmp/"+pos+"-resfusionmap-"+fname);
				for (int i=pos;i<nombreMaps;i=i+nombreDemons) {
					input = new FileInputStream("/tmp/"+i+"-resmap-"+fname);
					int a;
					while ((a = input.read()) != -1) {
						output.write(a);
					}
				}
				System.out.println("Rassemblage des maps locaux termine");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			super.runReduce(this.mr,this.reader,this.writer,this.cb);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	

}


