package firstHadoop;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class covid {
	public static int countCountry = 0;
	public static HashMap<String, String> hm = new HashMap<String, String>();
	public static class CovidMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] cols = line.split(",");

			String country = cols[0].trim();
			hm.put(country, country);
			context.write(new Text(country), new Text(value));

		}
	}

	public static class CovidReducer extends Reducer<Text, Text, Text, Text> {

		public int max = 0;
		public int totalMax = 0;
		protected void setup(Context context) throws IOException, InterruptedException {
			String country = "COUNTRY";
			String output = String.format("%s - %s","MAX", "DATE");
			context.write(new Text(country), new Text(output));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			String output = String.format("%d - %s", totalMax, new Date().toString());
			context.write(new Text(hm.size()+""), new Text(output));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			max = 0;
			String date = "";
			String writeValue = "";
			for (Text value : values) {
				String line = value.toString();

				String[] tokens = line.toString().split(",");

				if (!tokens[3].equals("") && Integer.parseInt(tokens[3]) > max)
					max = Integer.parseInt(tokens[3]);
					date = tokens[2];
					

			}
			
			totalMax += max;
			writeValue = String.format("%d - %s", max, date);
			context.write(key, new Text(writeValue));
		}

	}

	public static void main(String[] args) throws Exception {

		args = new String[] { "D:\\hadoop-env\\datafile\\input\\dataset.csv",
		"D:\\hadoop-env\\datafile\\outputcovid" };

		/* delete the output directory before running the job */
		FileUtils.deleteDirectory(new File(args[1]));

		if (args.length != 2) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", " - ");
		Job job = Job.getInstance(conf);
		job.setJobName("Covid");
		job.setJarByClass(covid.class);
		job.setMapperClass(CovidMapper.class);
		job.setReducerClass(CovidReducer.class);
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
