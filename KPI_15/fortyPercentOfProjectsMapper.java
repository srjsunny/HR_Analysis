package KPI_15;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class fortyPercentOfProjectsMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

	//Calculated using KPI 4
	
	public static long total_projects= 57042;
	
	
	@Override
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
	{
	
		// give the output of KPI 4 as input here
		
		// (dept.)           (no.of projects)
		//sales                   1550
		// IT                     1000
		
		String tokens[] = value.toString().split("\t");
		String dept = tokens[0];
		long dept_projects_no = Long.parseLong(tokens[1]);
		
		if(dept_projects_no > (0.4)*total_projects)
		{
			
			context.write(new Text(dept),NullWritable.get());
		}
	
		
		
		
	}
	
	
}
