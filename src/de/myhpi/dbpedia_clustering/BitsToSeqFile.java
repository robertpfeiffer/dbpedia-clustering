/* 
* Copyright (C) 2008 Stuart Sierra
* Copyright (C) 2009 Robert Pfeiffer
*
* Licensed under the Apache License, Version 2.0 (the "License"); you
* may not use this file except in compliance with the License. You
* may obtain a copy of the License at
* http:www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

	package de.myhpi.dbpedia_clustering;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.*;





public class BitsToSeqFile
{
	private File inputFile;
        private File outputFile;
	private File namesFile;
	private LocalSetup setup;

	/** Sets up Configuration and LocalFileSystem instances for
	* Hadoop.  Throws Exception if they fail.  Does not load any
	* Hadoop XML configuration files, just sets the minimum
	* configuration necessary to use the local file system.
	*/
	public BitsToSeqFile() throws Exception {
		this.setup = new LocalSetup();
	}

	/** Sets the input tar file. */
	public void setInput(File inputFile) {
		this.inputFile = inputFile;
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
		DataInputStream input = null;
		SequenceFile.Writer output = null;
		BufferedReader names = null;
		int size,byte_size;
		int count = 0;
		String name;
		try {
			input = openInputFile();
			output = openOutputFile();
			names = this.openNameFile();
			
			size =input.readInt();
			
			byte_size=size/8;
			if(size%8 != 0)
				byte_size++;
			
			System.out.println(" "+size+" "+byte_size);

			for (byte [] currentEntry = new byte [size];count < 10000; input.readFully(currentEntry)) {
				count +=1;

				name = names.readLine();
				Text key = new Text(name);
				BytesWritable value = new BytesWritable(currentEntry);
				System.out.println(name);
				output.append(key, value);
			}
		} finally {
			if (input != null) { input.close(); }
			if (output != null) { output.close(); }
		}
	}

	protected DataInputStream openInputFile() throws Exception {
		InputStream fileStream = new FileInputStream(inputFile);
		return new DataInputStream(fileStream);
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
			BitsToSeqFile me = new BitsToSeqFile();
			me.setInput(new File(args[0]));
			me.setOutput(new File(args[2]));
			me.setNameFile(new File(args[1])); 
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
