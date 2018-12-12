package KPI_6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class promotedAndLeftDriver {
	public static void main(String []args) throws Exception
	{
	Configuration conf = new Configuration();
	String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	if(otherArgs.length<2)
	{
		System.err.println("There is some error, provide two paths");
		System.exit(2);
	
	}
	Job job = new Job(conf,"KPI 6");
	job.setJarByClass(promotedAndLeftDriver.class);
	job.setMapperClass(promotedAndLeftMapper.class);
	job.setReducerClass(promotedAndLeftReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
	FileInputFormat.addInputPath(job, new Path (otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	System.exit(job.waitForCompletion(true)? 0:1);
		
		
		
	}
	
}
