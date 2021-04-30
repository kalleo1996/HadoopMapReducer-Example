package totalSalary;

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
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import partitioner.PartitionerMy;
import partitioner.PartitionerMy.CaderPartitioner;
import partitioner.PartitionerMy.MapClass;
import partitioner.PartitionerMy.ReduceClass;





public class Totalsalary {

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
	
	public static class ReduceClass extends Reducer<Text, Text, Text, IntWritable> {
		public int sum = 0;
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		max = -1;
		for (Text val : values) {
		String[] str = val.toString().split("\t", -3);
		
		sum = sum +Integer.parseInt(str[4]);
		}
		context.write(new Text(key), new IntWritable(sum));
		
		}
	
		
		
		}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Total Salary");
		job.setJarByClass(Totalsalary.class);
		args = new String[] { "D:\\hadoop-env\\datafile\\input\\partitioner.txt",
		"D:\\hadoop-env\\datafile\\partitioneroutput3" };
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
		//job.setNumReduceTasks(3);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	
	
}
