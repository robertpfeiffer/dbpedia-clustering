package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
		private int length;
		private Map<Text, BytesWritable> centers;

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();

				this.length = conf.getInt("subject.length", 1);
				/*
				 * localFiles = DistributedCache.getLocalCacheFiles(job); Path p
				 * = localFiles[0]; final FileSystem fs =
				 * FileSystem.getLocal(job); final Path qualified =
				 * p.makeQualified(fs);
				 */
				this.centers = new LinkedHashMap();
				Text key = new Text();
				BytesWritable value = new BytesWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem
						.get(conf), new Path("centers.seq"), conf);

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
				Distance distance = new ByteBitDistance();
				long minDistance = Long.MAX_VALUE;
				Text nearestCenter = null;
				
				for (Map.Entry<Text, BytesWritable> entry : this.centers
						.entrySet()) {
					long newDistance = 0;
					BytesWritable center = entry.getValue();

					newDistance = distance.between(center, subject);
					// System.out.println(key + " => " + entry.getKey()
					//		+ " distance: " + newDistance);

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
					.getInt("subject.length", 1);
		}

		public void reduce(Text key, Iterator<BytesWritable> values,
				Context context) throws IOException {

			try {
				int counts[] = new int[length];
				byte byte_counts[] = new byte[length];
				int num_subjects = 0;
				for (BytesWritable subject = values.next(); values.hasNext(); subject = values
						.next()) {
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

	public static class OutputReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {

		public void reduce(Text key, Iterator<BytesWritable> values,
				Context context) throws IOException, InterruptedException {

				context.write(key, new BytesWritable());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setInt("subject.length", 200); // TODO: Not Hardcode this

		if (args.length != 3) {
			System.err.println("Usage: k-means <center> <subjects> <out>");
			System.exit(2);
		}
		
		Path tempDir = new Path("k-means-temp");

		Job job = new Job(conf, "k-means");
		job.setJarByClass(KMeans.class);
		// DistributedCache.addCacheFile(new URI(args[0]), conf);
		job.setMapperClass(ClusterMapper.class);
		job.setCombinerClass(CenterReducer.class);
		job.setReducerClass(CenterReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, tempDir);
		
		// start k-means
		job.waitForCompletion(true);
		
		FileSystem.get(conf).copyFromLocalFile(tempDir.suffix("/part-r-00000"), new Path(args[0]));
		FileSystem.get(conf).delete(tempDir, true);
		
		Job outputJob = new Job(conf, "k-means Output");
		outputJob.setJarByClass(KMeans.class);
		// DistributedCache.addCacheFile(new URI(args[0]), conf);
		outputJob.setMapperClass(ClusterMapper.class);
		outputJob.setReducerClass(OutputReducer.class);
		outputJob.setInputFormatClass(SequenceFileInputFormat.class);
		outputJob.setOutputFormatClass(TextOutputFormat.class);
		outputJob.setOutputKeyClass(Text.class);
		outputJob.setOutputValueClass(BytesWritable.class);
		FileInputFormat.setInputPaths(outputJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(outputJob, new Path(args[2]));

		outputJob.waitForCompletion(true);
	}
}
