package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.Random;

public class DummySeqFile
{
	private Random random;
	private File outputFile;
	private File namesFile;
	private LocalSetup setup;

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

	public void setNameFile(File namesFile) {
		this.namesFile = namesFile;
	}

	protected BufferedReader openNameFile() throws Exception {
		return new BufferedReader(new InputStreamReader(new FileInputStream(namesFile)));
	}



	/** Performs the conversion. */
	public void execute() throws Exception {
		SequenceFile.Writer output = null;
		BufferedReader names = null;
		int size;
		String name;
		try {
			output = openOutputFile();
			names = this.openNameFile();
			size = 20;
			
			for (int i = 0; i < size; i++) {
				name = names.readLine();
				byte[] bytes = new byte[size*10];
				
				for (int k = 0; k < bytes.length; k++)
				{
					bytes[k] = (byte) random.nextInt(2);
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
			me.setOutput(new File(args[1]));
			me.setNameFile(new File(args[0])); 
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}