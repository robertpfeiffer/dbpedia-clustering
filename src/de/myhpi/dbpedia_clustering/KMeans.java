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
		private Distance distance;


		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				Path p;
				if (conf.getBoolean("run.local",false)) {
					p = new Path("k-means-temp-in");
				} else {
					p = DistributedCache.getLocalCacheFiles(conf)[0];
				}
				final FileSystem fs = FileSystem.getLocal(conf);
				final Path qualified = p.makeQualified(fs);

				this.centers = new LinkedHashMap();
				Text key = new Text();
				BytesWritable value = new BytesWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, qualified, conf);

				while (reader.next(key, value) == true) {
					this.centers.put(key, value);
					key = new Text();
					value = new BytesWritable();
				}
				reader.close();

				if (conf.get("distance.calculation").equals("Jaccard")) {
					distance = new JaccardDistance();
				} else {
					distance = new EuclideanDistance();
				}
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
					.getInt("subject.length", -1);
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
		private Distance distance;

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				Path p;
				if (conf.getBoolean("run.local",false)) {
					p = new Path("k-means-temp-in");
				} else {
					p = DistributedCache.getLocalCacheFiles(conf)[0];
				}
				final FileSystem fs = FileSystem.getLocal(conf);
				final Path qualified = p.makeQualified(fs);

				this.centers = new LinkedHashMap();
				Text key = new Text();
				BytesWritable value = new BytesWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, qualified, conf);

				while (reader.next(key, value) == true) {
					this.centers.put(key, value);
					key = new Text();
					value = new BytesWritable();
				}
				reader.close();

				if (conf.get("distance.calculation").equals("Jaccard")) {
					distance = new JaccardDistance();
				} else {
					distance = new EuclideanDistance();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void map(Text key, BytesWritable subject, Context context)
				throws IOException {
			try {
				Distance distance = new JaccardDistance();
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

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: k-means <center> <subjects> <out>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		conf.addResource(new Path("config.xml"));

		FileSystem hdfs = FileSystem.get(conf);

		Path tempInput = new Path("k-means-temp-in");
		Path tempOutput = new Path("k-means-temp-out");

		Path centerPath = new Path(args[0]);
		Path subjectPath = new Path(args[1]);
		Path outPath = new Path(args[2]);
		
		hdfs.rename(centerPath, tempInput);

		for(int i = 0; i<conf.getInt("iterations",1); i++) {
		    if (!conf.getBoolean("run.local",false)) {
			     DistributedCache.addCacheFile(tempInput.toUri(), conf);
		    }
		    Job job = new Job(conf, "k-means iteration "+(i+1));
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
		    if (!conf.getBoolean("run.local",false)) {
			    DistributedCache.purgeCache(conf);
		    }
		    hdfs.delete(tempInput, true);
		    hdfs.rename(
			hdfs.globStatus(
				  tempOutput.suffix("/part-*"))[0].getPath(),tempInput);
		    hdfs.delete(tempOutput, true);

		}
		if (!conf.getBoolean("run.local",false)) {
			DistributedCache.addCacheFile(tempInput.toUri(), conf);
		}
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
