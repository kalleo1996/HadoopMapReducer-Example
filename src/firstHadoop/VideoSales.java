package firstHadoop;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class VideoSales {
	public static class VideoMapper extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] features = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			String genre = features[3];
			context.write(new Text(genre),new Text(value));
		}
	}

	public static class VideoReducer extends Reducer<Text,Text,Text,FloatWritable>{
		private static TreeMap<FloatWritable,Text> btmGlobalSales = new TreeMap<FloatWritable,Text>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text record: values) {
				String[] features = record.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				String name = features[0];
				float globalSales = Float.parseFloat(features[9]);
				btmGlobalSales.put(new FloatWritable(globalSales), new Text(key+" "+name));
				if(btmGlobalSales.size() > 3) {
					btmGlobalSales.remove(btmGlobalSales.lastKey());
				}
			}
			for(Entry<FloatWritable, Text> entry : btmGlobalSales.entrySet()) {
				  FloatWritable rkey = entry.getKey();
				  Text value = entry.getValue();
				  context.write(value,rkey);  
			}
			btmGlobalSales.clear();
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] arguments = {"C:\\hadoop\\datafile\\input\\videosales.txt","C:\\hadoop\\datafile\\output\\videosales"};
		FileUtils.deleteDirectory(new File(arguments[1]));
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "VideoSales");
		job.setJarByClass(VideoSales.class);
		job.setMapperClass(VideoMapper.class);
		job.setReducerClass(VideoReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

}
