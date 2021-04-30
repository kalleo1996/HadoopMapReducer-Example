package firstHadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import firstHadoop.sumvalue.IntSumReducer;
import firstHadoop.sumvalue.TokenizerMapper;

public class bottom3ongener {

	public static class bottomMapper extends Mapper<Object, Text, Text, Text> {
int count =0;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			count++;
	
			String line = value.toString();
			String[] cols = line.split(",",5);
            
			String gener = cols[2].trim();
		if(count>=2)
			context.write(new Text(gener), new Text(value));
		
			

		}
	}

	public static class BottomReducer extends Reducer<Text, Text, NullWritable, Text> {
		
//		public int max = 0;
//		public int totalMax = 0;
		double total=0.0;
		protected void setup(Context context) throws IOException, InterruptedException {
//			String country = "genere";
//			String output = String.format("%s - %s","MAX", "DATE");
//			context.write(new Text(country), new Text(output));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
//			String output = String.format("%d - %s", totalMax, new Date().toString());
//			context.write(new Text(hm.size()+""), new Text(output));
			
//			for (String i : alltherecords) {
//				context.write(NullWritable.get(), new Text(i));
//			    }
//			for (Text t : TopRecord.descendingMap().values()) {
//				// Output our ten records to the file system with a null key
//				//alltherecords.add(t.toString());
			String na="total sales";
			String str2 = String.format("%s %f",na,total);
				context.write(NullWritable.get(), new Text(str2));
//				
//				}
			
			
			
			
		}
	//private ArrayList<String> alltherecords = new ArrayList<String>();
 
		//private TreeMap<Float, Text> TopRecord = new TreeMap<Float, Text>();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  
			TreeMap<Float, Text> TopRecordMap = new TreeMap<Float, Text>();
			for (Text value : values) {
				 
				String line=value.toString();
				if(line.length()>0){
				String[] tokens=line.split(",");
				//split the data and fetch salary
				if(!tokens[8].equals(""))
				{
					float sale=Float.parseFloat(tokens[8]);
					//insert salary as key and Salary as value
					//tree map sort the records based on salary
					TopRecordMap.put(sale, new Text(value));
					
					if(TopRecordMap.size()>3){
						TopRecordMap.remove(TopRecordMap.lastKey());
						}
					
					
				}
				}
				
				}
			
			
			
			for (Text t : TopRecordMap.descendingMap().values()) {
				// Output our ten records to the file system with a null key
				//alltherecords.add(t.toString());
				String lines=t.toString();
				String[] tokens=lines.split(",");
				float sales=Float.parseFloat(tokens[8]);
				String str = String.format("%f %s %s", sales, tokens[2],tokens[1]);
				total=total+sales;
				context.write(NullWritable.get(), new Text(str));
				}
			
			
			
		}
}
	
	 public static void main(String[] args) throws Exception {
//		 Configuration conf = new Configuration();
//		 Job job = Job.getInstance(conf, "word count");
//		 job.setJarByClass(wordcount.class);
//		 job.setMapperClass(TokenizerMapper.class);
//		 job.setCombinerClass(IntSumReducer.class);
//		 job.setReducerClass(IntSumReducer.class);
//		 job.setOutputKeyClass(IntWritable.class);
//		 job.setOutputValueClass(Text.class);
//		 FileInputFormat.addInputPath(job, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		
			 Configuration conf = new Configuration();
				Job job = Job.getInstance(conf, "videogame");
				job.setJarByClass(bottom3ongener.class);
				args = new String[] { "D:\\hadoop-env\\datafile\\input\\videogame.csv",
						"D:\\hadoop-env\\datafile\\output3videogame" };
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[1]));
				if (args.length != 2) {
					System.err.println("Please specify the input and output path");
					System.exit(-1);
				}
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.setMapperClass(bottomMapper.class);
				
				job.setReducerClass(BottomReducer.class);
				job.setNumReduceTasks(1);
				
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setOutputKeyClass(NullWritable.class);
				job.setOutputValueClass(Text.class);
				
				System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		 }
	
	
	
	
}
