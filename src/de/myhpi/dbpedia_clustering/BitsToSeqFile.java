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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.StreamTokenizer;



public class BitsToSeqFile {

    private File inputFile;
    private File outputFile;
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

    /** Performs the conversion. */
    public void execute() throws Exception {
        DataInputStream input = null;
        SequenceFile.Writer output = null;
	int size;
        try {
            input = openInputFile();
            output = openOutputFile();
	    size = (int) java.lang.Math.ceil(input.readInt()/8);
	    for (byte [] currentEntry = new byte [size];true; input.readFully(currentEntry))
	    {   
                Text key = new Text("bla");
                BytesWritable value = new BytesWritable(currentEntry);
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
            me.setOutput(new File(args[1]));
            me.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
