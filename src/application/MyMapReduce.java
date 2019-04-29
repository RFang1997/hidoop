package application;


import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import config.Project;
import map.MapReduce;
import ordo.Job;
import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import hdfs.HdfsClient;


public class MyMapReduce implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes word counts
	public void map(FormatReader reader, FormatWriter writer) {
		
		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
				else hm.put(tok, 1);
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
			else hm.put(kv.k, Integer.parseInt(kv.v));
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}
	
	public static void main(String args[]) {
		Format.Type fmt;
		if (args[0].equals("line")) {
			fmt = Format.Type.LINE;
		} else {
			fmt = Format.Type.KV;
		}
		HdfsClient.HdfsWrite(fmt, args[1], 1);
		Job j = new Job();
        j.setInputFormat(Format.Type.KV);
        j.setInputFname(args[1]);
        j.setNumberOfMaps(Project.ports.length);
       long t1 = System.currentTimeMillis();
		j.startJob(new MyMapReduce());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
