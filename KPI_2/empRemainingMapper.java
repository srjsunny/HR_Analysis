package KPI_2;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class empRemainingMapper extends Mapper<Object,Text,Text,IntWritable> {

	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		String dept = tokens[8];
		int whether_left_or_not=Integer.parseInt(tokens[6]);
		
		context.write(new Text(dept), new IntWritable(whether_left_or_not));
		
	}

}
