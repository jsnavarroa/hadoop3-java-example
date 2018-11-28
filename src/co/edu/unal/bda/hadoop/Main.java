package co.edu.unal.bda.hadoop;

import org.apache.hadoop.conf.Configuration;

public class Main {
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			//WordCount.main("biblia/input","biblia/p1");
			//WordRelevance.main("biblia/input", "biblia/p1");
			LetterWordCount.main("biblia/input", "biblia/p2");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9800");
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		return conf;
	}

}
