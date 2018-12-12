package KPI_4;

import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;




public class projectCountMapper extends Mapper<Object,Text,Text,LongWritable>{
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		String dept = tokens[8];
		long no_of_projects = Long.parseLong(tokens[2]);
		
		context.write(new Text(dept), new LongWritable(no_of_projects));
		
		
		
		
		
	}
}
