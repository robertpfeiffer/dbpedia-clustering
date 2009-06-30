package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.Random;

public class DummySeqFile
{
	private Random random;
	private File outputFile;
	private LocalSetup setup;
	
	public static final int SUBJECTS = 200;
	public static final int ATTRIBUTES = 800;
	public static final int GROUPS = 10;

	/** Sets up Configuration and LocalFileSystem instances for
	* Hadoop.  Throws Exception if they fail.  Does not load any
	* Hadoop XML configuration files, just sets the minimum
	* configuration necessary to use the local file system.
	*/
	public DummySeqFile() throws Exception {
		this.setup = new LocalSetup();
		this.random = new Random();
	}

	/** Sets the output SequenceFile. */
	public void setOutput(File outputFile) {
		this.outputFile = outputFile;
	}

	/** Performs the conversion. */
	public void execute() throws Exception {
		SequenceFile.Writer output = null;
		String name;
		int group = 0;
		
		try {
			output = openOutputFile();
			
			for (int i = 0; i < SUBJECTS; i++) {
				name = String.valueOf(i+1);
				byte[] bytes = new byte[ATTRIBUTES/8];
				group = i / (SUBJECTS / GROUPS);
				
				for (int k = 0; k < bytes.length; k++)
				{
					if (k / (bytes.length / GROUPS) == group) {
						bytes[k] = (byte) 0;
					} else {
						bytes[k] = Byteconverter.toSigned(random.nextInt(256));
					}
				}
				BytesWritable value = new BytesWritable(bytes);
				
				System.out.println(name+ " => " + value);
				output.append(new Text(name), value);
			}
		} finally {
			if (output != null) { output.close(); }
		}
	}

	protected SequenceFile.Writer openOutputFile() throws Exception {
		Path outputPath = new Path(outputFile.getAbsolutePath());
		return SequenceFile.createWriter(setup.getLocalFileSystem(), setup.getConf(),
			outputPath,
			Text.class, BytesWritable.class,
			SequenceFile.CompressionType.BLOCK);
	}

	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			DummySeqFile me = new DummySeqFile();
			me.setOutput(new File(args[0]));
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}