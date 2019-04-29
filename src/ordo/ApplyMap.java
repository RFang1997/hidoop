package ordo;

import java.rmi.RemoteException;

import formats.Format;
import map.MapReduce;

public class ApplyMap extends DaemonImpl implements Runnable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	MapReduce mr;
	Format reader;
	Format writer;
	CallBack cb;
	
	public ApplyMap(MapReduce mr, Format reader, Format writer, CallBack cb) throws RemoteException {
		this.mr = mr;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
	}
	
	public void run() {
		try {
			System.out.println("Application du map en cours ...");
			super.runMap(this.mr,this.reader,this.writer,this.cb);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	

}
