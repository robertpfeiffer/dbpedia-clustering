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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.io.*;

public class BitsToSeqFile
{
	private File inputFile;
    private String outputName;
	private File namesFile;
	protected LocalSetup setup;

	public BitsToSeqFile() throws Exception {
		this.setup = new LocalSetup();
	}

	/** Sets the input file. */
	public void setInput(File inputFile) {
		this.inputFile = inputFile;
	}

	/** Sets the output SequenceFile. */
	public void setOutput(String output) {
		this.outputName = output;
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
		String name = null;
		try {
			input = openInputFile();
			output = new SequenceFileHandler().openWriter(this.outputName);
			names = this.openNameFile();
			
			size = input.readInt();

			byte_size=size/8;
			if(size%8 != 0)
				byte_size++;
			
			byte [] currentEntry = new byte [byte_size];
			while ((input.read(currentEntry)) != -1) {
				count +=1;
				
				name = names.readLine();
				Text key = new Text(name);
				BytesWritable value = new BytesWritable(currentEntry);
				output.append(key, value);
				
				if (count % 10000 == 0) {
					System.out.println("processed entries: "+count);
				}
			}
			System.out.println(name);
			System.out.println("Number of subjects: "+count);
			System.out.println("Number of attributes: "+size);
		} finally {
			if (input != null) { input.close(); }
			if (output != null) { output.close(); }
		}
	}

	protected DataInputStream openInputFile() throws Exception {
		InputStream fileStream = new FileInputStream(inputFile);
		return new DataInputStream(fileStream);
	}

	/** Runs the converter at the command line. */
	public static void main(String[] args) {
		try {
			BitsToSeqFile me = new BitsToSeqFile();
			me.setInput(new File(args[0]));
			me.setOutput(args[2]);
			me.setNameFile(new File(args[1])); 
			me.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
