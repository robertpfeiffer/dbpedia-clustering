package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.Random;

public class BitsToSeqFileClusters extends BitsToSeqFile
{
	private Random random;
	private String subjectsFile;
	private int clusterNumber = 5;
	
	/** Sets up Configuration and LocalFileSystem instances for
	 * Hadoop.  Throws Exception if they fail.  Does not load any
	 * Hadoop XML configuration files, just sets the minimum
	 * configuration necessary to use the local file system.
	 */
	public BitsToSeqFileClusters() throws Exception {
		super();
		this.random = new Random();
	}
	
	protected SequenceFile.Reader openSubjectsFile() throws Exception {
		return new SequenceFile.Reader(setup.getLocalFileSystem(), new Path(this.subjectsFile), setup.getConf());
	}
	
	protected int getSequenceFileSize() throws Exception {
		int count = 0;
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		SequenceFile.Reader file = this.openSubjectsFile();
		
		while (file.next(key, value) == true) {
			count++;
		}
		
		file.close();
		return count;
	}

	/** Performs the conversion. */
	public void execute() throws Exception {
		SequenceFile.Reader subjects = null;
		SequenceFile.Writer output = null;
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		int count = 0;
		byte[] center;
		byte[] bytes;
		int size, i;
		
		try {
			subjects = this.openSubjectsFile();
			output = this.openOutputFile();
			size = this.getSequenceFileSize();
			
			i = 0;
			while (subjects.next(key, value)) {
				if (random.nextInt(size) < this.clusterNumber || (size-i == this.clusterNumber-count)) {
					center = value.getBytes();
					bytes = new byte[value.getLength()*8];
					
					for (int k = 0; k < bytes.length; k++) {
						bytes[k] = Byteconverter.bitToByte(Byteconverter.bitAt(center, k));
					}
					
					System.out.println(key + " => " + new BytesWritable(bytes));
					output.append(key, new BytesWritable(bytes));
					count++;
				}
				if (count == this.clusterNumber) 
					break;
				
				key = new Text();
				value = new BytesWritable();
				i++;
			}
		} finally {
			if (output != null) { output.close(); }
			if (subjects != null) { subjects.close(); }
		}
	}
	
	private void setClusterNumber(String string) {
		this.clusterNumber = Integer.parseInt(string);
	}
	
	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			BitsToSeqFileClusters me = new BitsToSeqFileClusters();
			me.subjectsFile = args[0];
			me.setInput(new File(me.subjectsFile));
			me.setOutput(new File(args[1]));
			if (args.length == 3) {
				me.setClusterNumber(args[2]);
			}
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}