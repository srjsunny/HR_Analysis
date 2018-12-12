package KPI_3;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static void main(String []args) throws Exception
	{
	Configuration conf = new Configuration();
	String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	if(otherArgs.length<2)
	{
		System.err.println("There is some error, provide two paths");
		System.exit(2);
	
	}
	Job job = new Job(conf,"kpi 3");
	job.setJarByClass(Driver.class);
	job.setMapperClass(avgMonthlyWorkingHourMapper.class);
	job.setReducerClass(avgMonthlyWorkingHourReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
	FileInputFormat.addInputPath(job, new Path (otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	System.exit(job.waitForCompletion(true)? 0:1);
		
		
		
	}

}