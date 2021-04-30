package firstHadoop;

import java.io.File;
import java.io.IOException;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class maxforcountry {

	public static class CovidMapClass extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException  {
		
		String[] str = value.toString().split(",");
		
		String country = str[0]; 
		                    //country0 //date1    //values2
//		String noOfVaccine=str[0]+","+str[2]+","+str[3];
		
		
			context.write(new Text(country), new Text(value));
		
		
		
		}
		}	
//	public static class CovidMapper extends Mapper<Object, Text, Text, Text> {
//
//		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			String line = value.toString();
//			String[] cols = line.split(",");
//
//			String country = cols[0];
//			context.write(new Text(country), new Text(value));
//
//		}
//	}
	
//	public static class CovidReduceClass extends Reducer<Text, Text, Text, Text> {
//		public int max = -1;
//		
//		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		  String date="";
//			max = -1;
//		
//		for (Text val : values) {
//		String[] str = val.toString().split(",");
//		if (!str[2].equals("") && Integer.parseInt(str[2]) > max)
//		{
//			max = Integer.parseInt(str[2]);
//		     
//		   date=str[1];
//		    
//		}
//		
//		
//		}
//		
//		context.write(new Text(key), new Text(max+"\t"+date));
//	
//		}
//	}
	
	public static class CovidReducer extends Reducer<Text, Text, Text, Text> {

		public int max = 0;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			max = 0;
			String date = "";
			for (Text value : values) {
				String line = value.toString();

				String[] tokens = line.split(",");

				if (!tokens[3].equals("") && Integer.parseInt(tokens[3]) > max) {
					max = Integer.parseInt(tokens[3]);
					date = tokens[2];
				}

			}
			context.write(new Text(key), new Text(max + "\t" + date));
		}

	}
	
	
	
	
	//D:\hadoop-env\datafile\input\dataset.csv D:\hadoop-env\datafile\outputcovid
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Covid Test");
		job.setJarByClass(maxforcountry.class);
		args = new String[] { "D:\\hadoop-env\\datafile\\input\\dataset.csv",
				"D:\\hadoop-env\\datafile\\outputcovid" };
		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[1]));
		if (args.length != 2) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(CovidMapClass.class);
		
		job.setReducerClass(CovidReducer.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	
	

	
	
}
