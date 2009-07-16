package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileHandler {
	private LocalSetup setup;
	
	public SequenceFileHandler() throws Exception {
		this.setup = new LocalSetup();
	}
	
	public SequenceFile.Reader openReader(String path) throws IOException {
		return new SequenceFile.Reader(setup.getLocalFileSystem(), new Path(path), setup.getConf());
	}
	
	public SequenceFile.Writer openWriter(String path) throws IOException {
		return SequenceFile.createWriter(setup.getLocalFileSystem(), setup.getConf(), new Path(path), Text.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK);
	}
	
	public int getReaderSize(String path) throws IOException {
		int count = 0;
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		SequenceFile.Reader file = this.openReader(path);
		
		while (file.next(key, value)) {
			count++;
		}
		
		file.close();
		return count;
	}
}
