package KPI_14;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;



public class experienceSalaryDistributionDriver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2)
		{
			System.err.println("Error provide two paths");
			System.exit(2);
		}
		Job job = new Job(conf, "kpi 14");
		job.setJobName("Experienced employee salary distributions");
		job.setJarByClass(experienceSalaryDistributionDriver.class);
		job.setMapperClass(experienceSalaryDistributionMapper.class);
		job.setReducerClass(experienceSalaryDistributionReducer.class);
		
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		
		
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	
	
	
	
	
	
	
	
	
	
	
}
