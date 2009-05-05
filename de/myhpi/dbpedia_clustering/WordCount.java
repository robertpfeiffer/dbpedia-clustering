package de.myhpi.dbpedia_clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

class Map extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key,
			Text value, 
			OutputCollector<Text, IntWritable> output, 
			Reporter reporter) 
		throws IOException
	{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) 
		{
			word.set(tokenizer.nextToken());
			output.collect(word, one);
		}
	}
}

class Reduce extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable>
{ 
	public void reduce(Text key, 
			   Iterator<IntWritable> values, 
			   OutputCollector<Text, IntWritable> output, 
			   Reporter reporter) 
		throws IOException
	{
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}
}
         
public class WordCount {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");
         
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
         
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
         
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
         
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
         
		JobClient.runJob(conf);
	}
         
}
