package firstHadoop;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class sumvalue {
	
	public static class TokenizerMapper
	 extends Mapper<Object, Text,IntWritable,Text>{
	 private final static IntWritable one = new IntWritable(1);
	 private Text word = new Text();
	 public void map(Object key, Text value, Context context
	 ) throws IOException, InterruptedException {
	 StringTokenizer itr = new StringTokenizer(value.toString());
	 while (itr.hasMoreTokens()) {
	 word.set(itr.nextToken());
	 context.write(one,word);
	 }
	 }
	 }
	 public static class IntSumReducer
	 extends Reducer<IntWritable,Text,NullWritable,Text> {
	 private IntWritable result = new IntWritable();
	 public void reduce(IntWritable key, Iterable<Text> values,
	 Context context
	 ) throws IOException, InterruptedException {
	 int sum = 0;
	 for (Text val : values) {
	 sum += Integer.parseInt(val.toString());
	 }
	 result.set(sum);
	 context.write(NullWritable.get(),new Text( result.toString()));
	 }
	 }
	 public static void main(String[] args) throws Exception {
//	 Configuration conf = new Configuration();
//	 Job job = Job.getInstance(conf, "word count");
//	 job.setJarByClass(wordcount.class);
//	 job.setMapperClass(TokenizerMapper.class);
//	 job.setCombinerClass(IntSumReducer.class);
//	 job.setReducerClass(IntSumReducer.class);
//	 job.setOutputKeyClass(IntWritable.class);
//	 job.setOutputValueClass(Text.class);
//	 FileInputFormat.addInputPath(job, new Path(args[0]));
//	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		 Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Covid Test");
			job.setJarByClass(maxforcountry.class);
			args = new String[] { "D:\\hadoop-env\\datafile\\input\\sum.txt",
					"D:\\hadoop-env\\datafile\\output3sum" };
			/* delete the output directory before running the job */
			FileUtils.deleteDirectory(new File(args[1]));
			if (args.length != 2) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
			}
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setMapperClass(TokenizerMapper.class);
			
			job.setReducerClass(IntSumReducer.class);
			job.setNumReduceTasks(1);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	 }
}
