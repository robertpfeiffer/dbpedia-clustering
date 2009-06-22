package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

class DBMap extends MapReduceBase
	implements Mapper<Text, BytesWritable, Text, BytesWritable>
{
	private Path[] localFiles;
	private int length;
	private Map<Text,BytesWritable> centers;

	public void configure(JobConf job) {
		try
		{
			this.length=job.getInt("subject.length",1);
/*			localFiles = DistributedCache.getLocalCacheFiles(job);
			Path p = localFiles[0];
			final FileSystem fs = FileSystem.getLocal(job);
			final Path qualified = p.makeQualified(fs);
*/
			centers = new LinkedHashMap();
			Text key = new Text();
			BytesWritable value = new BytesWritable();
			SequenceFile.Reader reader =
				new SequenceFile.Reader(FileSystem.get(job), new Path("centers.seq"), job);

			while (reader.next(key, value) == true)
			{
				centers.put(key,value);
				key = new Text();
				value = new BytesWritable();
			}
			reader.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public void map(Text key,
			BytesWritable subject,
			OutputCollector<Text, BytesWritable> output,
			Reporter reporter)
		throws IOException
	{
		try
		{
			Distance distance = new ByteBitDistance();
			long minDistance = Long.MAX_VALUE;
			Map.Entry<Text,BytesWritable> current = null;
			
			System.out.println("++");
			
			for(Map.Entry<Text,BytesWritable> entry:this.centers.entrySet())
			{
				long newDistance = 0;
				byte [] center = entry.getValue().getBytes();
				
				assert(length == center.length);
				
				newDistance = distance.between(center, subject.getBytes());
				if (newDistance < minDistance)
				{
					minDistance = newDistance;
					current = entry;
				}
				
				System.out.println(entry.getKey() + " distance: " + distance);
			}
			
			output.collect(current.getKey(), current.getValue());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

class DBReduce extends MapReduceBase
	implements Reducer<Text, BytesWritable, Text, BytesWritable>
{
	private int length;

	public void configure(JobConf job) {
		this.length=job.getInt("subject.length",1);
	}

	public void reduce(Text key, 
		Iterator<BytesWritable> values, 
		OutputCollector<Text, BytesWritable> output, 
		Reporter reporter) 
		throws IOException
	{
	
		try
		{
			int counts[] = new int[length];
			byte byte_counts[] = new byte[length];
			int num_subjects = 0;
			for(BytesWritable subject=values.next();values.hasNext();
			    subject=values.next())
			{
				byte bits[]=subject.getBytes();
				num_subjects++;
				for (int i= 0;i<length;i++)
					counts[i] += Byteconverter.bitAt(bits,i);
			}
			for (int i= 0;i<length;i++)
			{
				byte_counts[i]=Byteconverter.ratioToByte(counts[i],num_subjects);
			}
			output.collect(key, new BytesWritable(byte_counts));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

public class K_Means {
	public static void main(String[] args) throws Exception
	{
		JobConf conf = new JobConf(K_Means.class);
		conf.setJobName("k-means");
		conf.setInt("subject.length",42644); //TODO: Not Hardcode this

		// DistributedCache.addCacheFile(new URI(args[0]), conf);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BytesWritable.class);

		conf.setMapperClass(DBMap.class);
		conf.setReducerClass(DBReduce.class);

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		//conf.setOutputFormat(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
	}
}
