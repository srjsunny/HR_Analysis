package KPI_6;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class promotedAndLeftMapper extends Mapper<Object,Text,Text,LongWritable> {
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		
		String dept = tokens[8];
		int promoted = Integer.parseInt(tokens[7]);    //'1' if promoted
		int left = Integer.parseInt(tokens[6]);             // '1' if left the company
		
		//sum = 2 , if promoted and left the company
		
		int sum = promoted+left;
		context.write(new Text(dept),new LongWritable(sum));
		
		
		
		
	}
}
