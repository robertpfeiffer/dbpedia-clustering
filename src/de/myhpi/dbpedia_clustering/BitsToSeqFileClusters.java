package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.*;
import java.io.*;

import java.util.Random;

public class BitsToSeqFileClusters extends BitsToSeqFile
{
	private Random random;
	private int numerator;
	private int denominator;
	
	/** Sets up Configuration and LocalFileSystem instances for
	 * Hadoop.  Throws Exception if they fail.  Does not load any
	 * Hadoop XML configuration files, just sets the minimum
	 * configuration necessary to use the local file system.
	 */
	public BitsToSeqFileClusters() throws Exception {
		super();
		this.random = new Random();
	}

	public void setProb(int numerator, int denominator){
		this.numerator = numerator;
		this.denominator = denominator;
	}

	/** Performs the conversion. */
	public void execute() throws Exception {
		DataInputStream input = null;
		SequenceFile.Writer output = null;
		BufferedReader names = null;
		int size,byte_size;
		int count = 0;
		String name;
		try {
			input = this.openInputFile();
			output = this.openOutputFile();
			names = this.openNameFile();
			
			size =input.readInt();
			
			byte_size=size/8;
			if(size%8 != 0)
				byte_size++;
			
			System.out.println(" "+size+" "+byte_size);
			
			System.out.println(" ++ ");
			
			for (byte [] bits = new byte [byte_size];count < 10; input.readFully(bits))
			{
				name = names.readLine();
				if(random.nextInt(denominator)<numerator)
				{
					count += 1;

					byte [] bytes = new byte[size];
					for (int i= 0;i<size;i++)
					{
						bytes[i] = Byteconverter.toSigned(
							Byteconverter.bitToByte(Byteconverter.bitAt(bits,i)));
					}
					Text key = new Text(name);
					BytesWritable value = new BytesWritable(bytes);
									System.out.println(name);

					output.append(key, value);
				}
			}
		} finally {
			if (input != null) { input.close(); }
			if (output != null) { output.close(); }
		}
	}
	
	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			BitsToSeqFileClusters me = new BitsToSeqFileClusters();
			me.setInput(new File(args[0]));
			me.setOutput(new File(args[2]));
			me.setNameFile(new File(args[1])); 
			me.setProb(1,100);
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}