package de.myhpi.dbpedia_clustering;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.*;
import java.io.*;

import java.util.Random;

public class BitsToSeqFileClusters extends BitsToSeqFile{

    private Random random;
    private int numerator;
    private int denominator;
    private File names;

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

    public void setNameFile(File nameFile) {
        this.names = nameFile;
    }

    protected BufferedReader openNameFile() throws Exception {
        return new BufferedReader(new InputStreamReader(new FileInputStream(names)));
    }


    /** Performs the conversion. */
    public void execute() throws Exception {
        DataInputStream input = null;
        SequenceFile.Writer output = null;
	BufferedReader names = null;
	int size;
	int count = 0;
        try {
            input = this.openInputFile();
            output = this.openOutputFile();
	    names = this.openNameFile();
	    String name;
	    int length_=input.readInt();
	    size = (int) java.lang.Math.ceil(length_/8.0);
	    System.out.println(length_);System.out.println(size);
	    byte [] bytes = new byte[length_];
	    for (byte [] bits = new byte [size];count < 10; input.readFully(bits))
	    {
		name = names.readLine();
		if(random.nextInt(denominator)<numerator)
		{
			count += 1;
			for (int i= 0;i<length_;i++)
			{
				bytes[i] = Byteconverter.bitToByte(Byteconverter.bitAt(bits,i));
				if(bytes[i] == -1)
				{
					System.out.printf("%5x %5d %d \n",bits[i/8],bytes[i],i);
				}
			}
			Text key = new Text(name);
			BytesWritable value = new BytesWritable(bytes);
			
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