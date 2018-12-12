package KPI_9;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class myDriver {
	
	public static void main(String []args) throws Exception
	{
	Configuration conf = new Configuration();
	String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	if(otherArgs.length<2)
	{
		System.err.println("There is some error, provide two paths");
		System.exit(2);
	
	}
	Job job = new Job(conf,"KPI 9");
	job.setJarByClass(myDriver.class);
	job.setMapperClass(myMapper.class);
	job.setReducerClass(myReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job, new Path (otherArgs[0]));
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
	System.exit(job.waitForCompletion(true)? 0:1);
		
		
		
	}
	
	
	
	
	

}
