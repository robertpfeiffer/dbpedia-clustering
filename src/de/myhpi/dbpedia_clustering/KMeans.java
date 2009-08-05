package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {

	public static class ClusterMapper extends
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		private Path[] localFiles;
		private Map<Text, BytesWritable> centers;
		private Distance<BytesWritable, BytesWritable> distance;

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				if (conf.get("kmeans.distance.calculation").equals("Jaccard")) {
					distance = new JaccardDistance();
				} else {
					distance = new EuclideanDistance();
				}
				
				Path p;
				if (conf.getBoolean("kmeans.run.local",false)) {
					p = new Path("k-means-temp-in");
				} else {
					p = DistributedCache.getLocalCacheFiles(conf)[0];
				}
				final FileSystem fs = FileSystem.getLocal(conf);
				final Path qualified = p.makeQualified(fs);
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, qualified, conf);
				
				this.centers = new LinkedHashMap();
				Text key = new Text();
				BytesWritable value = new BytesWritable();

				while (reader.next(key, value) == true) {
					this.centers.put(key, value);
					key = new Text();
					value = new BytesWritable();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(Text key, BytesWritable subject, Context context)
				throws IOException {
			try {
				double minDistance = Double.MAX_VALUE;
				Text nearestCenter = null;
				
				for (Map.Entry<Text, BytesWritable> entry : this.centers
						.entrySet()) {
					double newDistance = 0;
					BytesWritable center = entry.getValue();

					newDistance = distance.between(center, subject);

					if (newDistance < minDistance) {
						minDistance = newDistance;
						nearestCenter = entry.getKey();
					}
				}

				context.write(nearestCenter, subject);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class CenterReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {
		private int length;

		protected void setup(Context context) {
			this.length = context.getConfiguration()
					.getInt("kmeans.subject.length", -1);
		}

		public void reduce(Text key, Iterable<BytesWritable> values,
				Context context) throws IOException {

			try {
				int counts[] = new int[length];
				byte byte_counts[] = new byte[length];
				int num_subjects = 0;
				
				for (BytesWritable subject : values) {
					byte bits[] = subject.getBytes();
					num_subjects++;
					for (int i = 0; i < length; i++)
						counts[i] += Byteconverter.bitAt(bits, i);
				}
				for (int i = 0; i < length; i++) {
					byte_counts[i] = Byteconverter.ratioToByte(counts[i],
							num_subjects);
				}
				context.write(key, new BytesWritable(byte_counts));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static class OutputMapper extends
			Mapper<Text, BytesWritable, Text, Text> {
		private Path[] localFiles;
		private Map<Text, BytesWritable> centers;
		private Distance<BytesWritable, BytesWritable> distance;

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				if (conf.get("kmeans.distance.calculation").equals("Jaccard")) {
					distance = new JaccardDistance();
				} else {
					distance = new EuclideanDistance();
				}
				
				Path p;
				if (conf.getBoolean("kmeans.run.local",false)) {
					p = new Path("k-means-temp-in");
				} else {
					p = DistributedCache.getLocalCacheFiles(conf)[0];
				}
				final FileSystem fs = FileSystem.getLocal(conf);
				final Path qualified = p.makeQualified(fs);
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, qualified, conf);
				
				this.centers = new LinkedHashMap();
				Text key = new Text();
				BytesWritable value = new BytesWritable();

				while (reader.next(key, value) == true) {
					this.centers.put(key, value);
					key = new Text();
					value = new BytesWritable();
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(Text key, BytesWritable subject, Context context)
				throws IOException {
			try {
				double minDistance = Double.MAX_VALUE;
				Text nearestCenter = null;
				
				for (Map.Entry<Text, BytesWritable> entry : this.centers
						.entrySet()) {
					double newDistance = 0;
					BytesWritable center = entry.getValue();

					newDistance = distance.between(center, subject);

					if (newDistance < minDistance) {
						minDistance = newDistance;
						nearestCenter = entry.getKey();
					}
				}

				context.write(nearestCenter, key);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class OutputReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for(Text value : values)
		    	context.write(key, value);
		}
	}
	
	public static boolean breakCondition(Configuration conf, Path oldCentersPath, Path newCentersPath) throws Exception {
		Distance distance;
		SequenceFile.Reader oldCentersReader;
		SequenceFile.Reader newCentersReader;
		LinkedHashMap newCenters = new LinkedHashMap();
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		double dissimilarity = 0.0;
		FileSystem hdfs = FileSystem.get(conf);
		
		if (conf.get("kmeans.distance.calculation").equals("Jaccard")) {
			distance = new JaccardDistance();
		} else {
			distance = new EuclideanDistance();
		}
		
		// new centers
		newCentersReader = new SequenceFile.Reader(hdfs, newCentersPath.makeQualified(hdfs), conf);
		while (newCentersReader.next(key, value) == true) {
			newCenters.put(key, value);
			key = new Text();
			value = new BytesWritable();
		}
		newCentersReader.close();
		
		// calculate sum of dissimilarity
		oldCentersReader = new SequenceFile.Reader(hdfs, oldCentersPath.makeQualified(hdfs), conf);
		while (oldCentersReader.next(key, value) == true) {
			dissimilarity += distance.index(value, newCenters.get(key));
		}
		oldCentersReader.close();

		// calculate average dissimilarity
		dissimilarity = dissimilarity / newCenters.size();
		
		System.out.println("Dissimilarity: " + dissimilarity + " < " + conf.getFloat("kmeans.breakcondition.dissimilarity", (float) 0.05));
		return dissimilarity < conf.getFloat("kmeans.breakcondition.dissimilarity", (float) 0.05);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: k-means <center> <subjects> <out>");
			System.exit(-1);
		}

		// load config
		Configuration conf = new Configuration();
		conf.addResource(new Path("config.xml"));

		// prepare temp directories
		FileSystem hdfs = FileSystem.get(conf);
		Path tempInput = new Path("k-means-temp-in");
		Path tempOutput = new Path("k-means-temp-out");
		Path centerPath = new Path(args[0]);
		Path subjectPath = new Path(args[1]);
		Path outPath = new Path(args[2]);
		Path tempOutputFile;
		hdfs.rename(centerPath, tempInput);
		
		// map/reduce Job
		int iteration = 1;
		boolean done = false;
		do {
		    if (!conf.getBoolean("kmeans.run.local",false)) {
			     DistributedCache.addCacheFile(tempInput.toUri(), conf);
		    }
		    Job job = new Job(conf, "k-means iteration " + iteration);
		    job.setJarByClass(KMeans.class);
		    job.setMapperClass(ClusterMapper.class);
		    job.setReducerClass(CenterReducer.class);
		    job.setInputFormatClass(SequenceFileInputFormat.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(BytesWritable.class);
		    FileInputFormat.setInputPaths(job, subjectPath);
		    FileInputFormat.setMaxInputSplitSize(job, conf.getInt("kmeans.split.size", 1000000));
		    FileOutputFormat.setOutputPath(job, tempOutput);
		
		    job.waitForCompletion(true);
		    
		    // get the path to the outputfile
		    tempOutputFile = hdfs.globStatus(tempOutput.suffix("/part-*"))[0].getPath();
		    
		    // break condition
		    if ((conf.get("kmeans.breakcondition").equals("dissimilarity") && breakCondition(conf, tempInput, tempOutputFile))
		    	|| iteration >= conf.getInt("kmeans.breakcondition.iterations",1)) {
		    	done = true;
		    }
		    
		    if (!conf.getBoolean("kmeans.run.local",false)) {
			    DistributedCache.purgeCache(conf);
		    }
		    hdfs.delete(tempInput, true);
		    hdfs.rename(tempOutputFile,tempInput);
		    hdfs.delete(tempOutput, true);
		    
		    iteration++;
		} while (!done);
		
		if (!conf.getBoolean("kmeans.run.local",false)) {
			DistributedCache.addCacheFile(tempInput.toUri(), conf);
		}
		
		// output Job
		Job outputJob = new Job(conf, "k-means Output");
		outputJob.setJarByClass(KMeans.class);
		outputJob.setMapperClass(OutputMapper.class);
		outputJob.setReducerClass(OutputReducer.class);
		outputJob.setInputFormatClass(SequenceFileInputFormat.class);
		outputJob.setOutputFormatClass(TextOutputFormat.class);
		outputJob.setOutputKeyClass(Text.class);
		outputJob.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(outputJob, subjectPath);
		FileInputFormat.setMaxInputSplitSize(outputJob, conf.getInt("kmeans.split.size", 1000000));
		FileOutputFormat.setOutputPath(outputJob, outPath);

		outputJob.waitForCompletion(true);
		hdfs.delete(tempInput, true);
	}
}
