package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

class DBMapReduce extends MapReduceBase 
	implements Mapper<Text, BytesWritable, Text, BytesWritable> 
{
	private Path[] localFiles;
	private int length;
	private Map<Text,BytesWritable> centers;

	public void configure(JobConf job) {
		localFiles = DistributedCache.getLocalCacheFiles(job);
		centers = new TreeMap();
		for(Path path:localFiles)
		{
			Text key = new Text();
			BytesWritable value = new BytesWritable();
			SequenceFile.Reader reader = 
				new SequenceFile.Reader(FileSystem.get(job), path, job);			
			while (reader.next(key, value) == true) {
				centers.put(key,value);
			}
			reader.close();	
		}
	}

	public void map(Text key,
			BytesWritable subject, 
			OutputCollector<Text, BytesWritable> output, 
			Reporter reporter) 
		throws IOException
	{
		int distance;
		int maxdistance = 256*length+1;
		Map.Entry<Text,BytesWritable> current = null;
		byte bits[]=subject.getBytes();

		for(Map.Entry<Text,BytesWritable> entry:this.centers.entrySet())
		{
			distance = 0;
			byte [] center =entry.getValue().getBytes(); 
			for (int i= 0;i<length;i++)
				distance += Math.abs(center[i]-
						 255*(1 & (bits[i/8] >> i%8)));
			if (distance<maxdistance)
				current=entry;
		}
		output.collect(current.getKey(), current.getValue());
	}
}

class DBReduce extends MapReduceBase 
	implements Reducer<Text, BytesWritable, Text, BytesWritable>
{ 
	private int length;
	public void reduce(Text key, 
			   Iterator<BytesWritable> values, 
			   OutputCollector<Text, BytesWritable> output, 
			   Reporter reporter) 
		throws IOException
	{
		int counts[] = new int[length];
		int num_subjects = 0;
		for(BytesWritable subject;values.hasNext();
		    subject=values.next())
		{
			byte bits[]=subject.getBytes();
			num_subjects++;
			for (int i= 0;i<length;i++)
				counts[i] += 1 & (bits[i/8] >> i%8);
		}
		byte byte_counts[] = new byte[length];
		for (int i= 0;i<length;i++)
		{
			byte_counts[i]=(255*counts[i])/num_subjects;
		}
		output.collect(key, new BytesWritable(byte_counts));
	}
}
         
public class K_Means {
	public int run(String[] args) throws Exception {	}
         
}
