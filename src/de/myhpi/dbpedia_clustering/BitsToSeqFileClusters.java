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

    protected StreamTokenizer openNameFile() throws Exception {
	Reader r = new BufferedReader(new InputStreamReader(new FileInputStream(names)));
        return new StreamTokenizer(r);
    }


    /** Performs the conversion. */
    public void execute() throws Exception {
        DataInputStream input = null;
        SequenceFile.Writer output = null;
	StreamTokenizer names = null;
	int size;
        try {
            input = this.openInputFile();
            output = this.openOutputFile();
	    names = this.openNameFile();

	    size = (int) java.lang.Math.ceil(input.readInt()/8.0);
	    byte [] bytes = new byte[size * 8];
	    for (byte [] bits = new byte [size];true; input.readFully(bits))
	    {
		names.nextToken();
		for (int i= 0;i<bytes.length;i++)
		    bytes[i] += 255*(1 & (bits[i/8] >> i%8));
		if(random.nextInt(denominator)<numerator){
		    Text key = new Text(names.toString());
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
	    me.setProb(1,1000);
            me.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}