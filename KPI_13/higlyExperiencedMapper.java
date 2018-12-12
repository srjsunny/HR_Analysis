package KPI_13;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class higlyExperiencedMapper extends Mapper<Object,Text,Text,IntWritable>{

	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
      String []tokens = value.toString().split(",");
      String dept = tokens[8];
      
      int exp = Integer.parseInt(tokens[4]);
      
      context.write(new Text(dept), new IntWritable(exp));
		
		
		
		
	}
	
	
}
