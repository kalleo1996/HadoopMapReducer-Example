package firstHadoop;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class covidvaccine {
	
	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of employees salary’s to the record
		private TreeMap<Float, Text> ToRecordMap = new TreeMap<Float, Text>() ;
		public void map(Object key, Text value, Context context)throws IOException,
		InterruptedException {
		String line=value.toString();
		
		
		System.out.print(line);
		
		String[] tokens=line.split("\t");
		//split the data and fetch salary
		if(tokens.length ==5 ) {
			System.out.println(tokens);
			float volumeofvaccine=Float.parseFloat(tokens[3]);
			//insert salary as key and entire row as value
			//tree map sort the records based on salary
			String val=tokens[0]+"\t"+tokens[2]+"\t"+tokens[3];
			
			
			ToRecordMap.put(volumeofvaccine, new Text(val.toString()));
			// If we have more than ten records, remove the one with the lowest salary
			// As this tree map is sorted in ascending order, the employee with
			// the lowest salary is the first key.
			if(ToRecordMap.size()>5){
			ToRecordMap.remove(ToRecordMap.lastKey());
			}	
		}
	
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
		// Output top ten records to the reducers with a null key
		for (Text t : ToRecordMap.values()) {
		context.write(NullWritable.get(), t);
		}
		}
		}
	
	public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
		private TreeMap<Float, Text> TopRecordMap = new TreeMap<Float, Text>();
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws
		IOException, InterruptedException {
		for (Text value : values) {
		String line=value.toString();
		if(line.length()>0){
		String[] tokens=line.split("\t");
		//split the data and fetch salary
		float volumeofvaccine=Float.parseFloat(tokens[2]);
		//insert salary as key and Salary as value
		//tree map sort the records based on salary
		TopRecordMap.put(volumeofvaccine, new Text(value));
		}
		}
		// If we have more than ten records, remove the one with the lowest salary
		// As this tree map is sorted in ascending order, the user with
		// the lowest salary is the first key.
		if(TopRecordMap.size()>5){
		TopRecordMap.remove(TopRecordMap.lastKey());
		}
		for (Text t : TopRecordMap.descendingMap().values()) {
		// Output our ten records to the file system with a null key
		context.write(NullWritable.get(), t);
		}
		}
		}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		 Job job = Job.getInstance(conf, "covid");
		 job.setJarByClass(covidvaccine.class);
		 job.setMapperClass(TopTenMapper.class);
		// job.setCombinerClass(TopTenReducer.class);
		 job.setReducerClass(TopTenReducer.class);
		 job.setMapOutputKeyClass(NullWritable.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(NullWritable.class);
		 job.setOutputValueClass(Text.class);
		 FileInputFormat.addInputPath(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
