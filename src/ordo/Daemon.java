package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.MapReduce;
import map.Mapper;
import map.Reducer;
import formats.Format;

public interface Daemon extends Remote {
	public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException;

	public void runReduce(Reducer r, Format reader, Format writer, CallBack cb) throws RemoteException;

	public void setListeThreadsmap(MapReduce mr, Format reader, Format writer, CallBack cb_m) throws RemoteException;

	public void lancerThreads() throws RemoteException;

	public void joinThreads() throws RemoteException;

	public void setReduceLocal(MapReduce mr, Format reader, Format writer, CallBack cb_rl, String inputFname, int i,
			int numberofmaps, int numberofdemons) throws RemoteException;

	public void lancerReduceLocal() throws RemoteException;

	public void joinReduceLocal() throws RemoteException;

}
