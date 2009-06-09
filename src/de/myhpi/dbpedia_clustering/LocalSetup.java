package de.myhpi.dbpedia_clustering;

/* From hadoop-*-core.jar, http://hadoop.apache.org/
 * Developed with Hadoop 0.16.3. */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.DataInputStream;

 public class LocalSetup {

    private FileSystem fileSystem;
    private Configuration config;

    /** Sets up Configuration and LocalFileSystem instances for
     * Hadoop.  Throws Exception if they fail.  Does not load any
     * Hadoop XML configuration files, just sets the minimum
     * configuration necessary to use the local file system.
     */
    public LocalSetup() throws Exception {
        config = new Configuration();
	
        /* Normally set in hadoop-default.xml, without it you get
         * "java.io.IOException: No FileSystem for scheme: file" */
        config.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
	
        fileSystem = FileSystem.get(config);
        if (fileSystem.getConf() == null) {
            /* This happens if the FileSystem is not properly
             * initialized, causes NullPointerException later. */
            throw new Exception("LocalFileSystem configuration is null");
        }
    }

    /** Returns a Hadoop Configuration instance for use in Hadoop API
     * calls. */
    public Configuration getConf() {
        return config;
    }

    /** Returns a Hadoop FileSystem instance that provides access to
     * the local filesystem. */
    public FileSystem getLocalFileSystem() {
        return fileSystem;
    }
}

