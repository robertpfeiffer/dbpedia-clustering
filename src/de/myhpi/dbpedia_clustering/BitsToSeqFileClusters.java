package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.Random;

public class BitsToSeqFileClusters extends BitsToSeqFile
{
	private Random random;
	private String subjectsFile;
	
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

	/** Performs the conversion. */
	public void execute() throws Exception {
		SequenceFile.Reader subjects = null;
		SequenceFile.Writer output = null;
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		int count = 0;
		byte[] center;
		byte[] bytes;
		
		try {
			subjects = this.openSubjectsFile();
			output = this.openOutputFile();
			
			while (subjects.next(key, value)) {
				if (count < 5) {
					center = value.getBytes();
					bytes = new byte[value.getLength()*8];
					
					for (int i = 0; i < bytes.length; i++) {
						bytes[i] = Byteconverter.bitToByte(Byteconverter.bitAt(center, i));
					}
					
					System.out.println(key + " => " + new BytesWritable(bytes));
					output.append(key, new BytesWritable(bytes));
				} else {
					break;
				}
				key = new Text();
				value = new BytesWritable();
				count++;
			}
		} finally {
			if (output != null) { output.close(); }
			if (subjects != null) { subjects.close(); }
		}
	}
	
	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			BitsToSeqFileClusters me = new BitsToSeqFileClusters();
			me.subjectsFile = args[0];
			me.setInput(new File(me.subjectsFile));
			me.setOutput(new File(args[1]));
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}