package KPI_11;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class experienceWiseEmpLeftMapper extends Mapper<Object,Text,IntWritable,IntWritable> {

	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		
		int exp = Integer.parseInt(tokens[4]);
		int isleft = Integer.parseInt(tokens[6]);
		
		context.write(new IntWritable(exp), new IntWritable(isleft));
	
		
		
		
		
		
		
	}
	
	
	
}
