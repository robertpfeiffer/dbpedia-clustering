package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.Random;

public class GenerateClusters extends BitsToSeqFile
{
	private Random random;
	private String subjectsFile;
	private int clusterNumber = 5;
	private int byte_size;
	
	/** Sets up Configuration and LocalFileSystem instances for
	 * Hadoop.  Throws Exception if they fail.  Does not load any
	 * Hadoop XML configuration files, just sets the minimum
	 * configuration necessary to use the local file system.
	 */
	public GenerateClusters() throws Exception {
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
		BytesWritable newValue = new BytesWritable();
		int count = 0;
		byte[] center;
		byte[] bytes;
		int size, i;
		
		try {
			subjects = this.openSubjectsFile();
			output = this.openOutputFile();
			
			System.out.println("start counting subjects");
			size = this.getSequenceFileSize();
			System.out.println("number of subjects: "+size);
			
			i = 0;
			while (subjects.next(key, value)) {
				if (random.nextInt(size) < this.clusterNumber || (size-i == this.clusterNumber-count)) {
					center = value.getBytes();
					bytes = new byte[byte_size];
					
					for (int k = 0; k < bytes.length; k++) {
						bytes[k] = Byteconverter.byteAt(center, k);
					}
					newValue = new BytesWritable(bytes);
					newValue.setSize(bytes.length);
					
					output.append(key, newValue);
					count++;
					System.out.println("cluster taken at " +i+ " : "+key);
				}
				
				if (count == this.clusterNumber) 
					break;
				
				if (i % 100000 == 0)
					System.out.println("processed subjects: "+i);
				
				key = new Text();
				value = new BytesWritable();
				i++;
			}
			System.out.println("finished");
		} finally {
			if (output != null) { output.close(); }
			if (subjects != null) { subjects.close(); }
		}
	}
	
	private void setClusterNumber(String string) {
		this.clusterNumber = Integer.parseInt(string);
	}
	
	private void setByteSize(String string) {
		this.byte_size = Integer.parseInt(string);
	}
	
	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			GenerateClusters me = new GenerateClusters();
			me.subjectsFile = args[0];
			me.setInput(new File(me.subjectsFile));
			me.setOutput(new File(args[1]));
			me.setByteSize(args[2]);
			if (args.length == 4) {
				me.setClusterNumber(args[3]);
			}
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}