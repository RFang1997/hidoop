package hdfs;

import java.io.ObjectInputStream;

public interface DataNodeInterface {
	
	public void receive(String fname, int numero, ObjectInputStream ois);
	
	public void send(String fname, int numero);
	
	public void delete(String fname, int numero);

}
