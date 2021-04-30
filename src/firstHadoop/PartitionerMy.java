package firstHadoop;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class PartitionerMy {
   
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable key, Text value, Context context) {
		try {
		String[] str = value.toString().split("\t", -3);
		
		String gender = str[3]; 
		context.write(new Text(gender), new Text(value));
		} catch (Exception e) {
		System.out.println(e.getMessage());
		}
		
		}
		}
		public static class CaderPartitioner extends Partitioner<Text, Text> {
		@Override public int getPartition(Text key, Text value, int numReduceTasks) {
		String[] str = value.toString().split("\t");
		int age = Integer.parseInt(str[2]);
		if (numReduceTasks == 0) {
		return 0;
		}
		if (age <= 20) {
		return 0;
		} else if (age > 20 && age <= 30) {
		return 1 % numReduceTasks;
		} else {
		return 2 % numReduceTasks;
		}
		}
		}
		public static class ReduceClass extends Reducer<Text, Text, Text, IntWritable> {
		public int max = -1;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		max = -1;
		for (Text val : values) {
		String[] str = val.toString().split("\t", -3);
		if (Integer.parseInt(str[4]) > max)
		max = Integer.parseInt(str[4]);
		}
		context.write(new Text(key), new IntWritable(max));
		}
		}
		
		public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top Salary");
		job.setJarByClass(PartitionerMy.class);
		args = new String[] { "D:\\hadoop-env\\datafile\\input\\partitioner.txt",
		"D:\\hadoop-env\\datafile\\partitioneroutput2" };
		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[1]));
		if (args.length != 2) {
		System.err.println("Please specify the input and output path");
		System.exit(-1);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MapClass.class);
		// set partitioner statement
		job.setPartitionerClass(CaderPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(3);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
}
