package KPI_10;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;



public class salaryWiseDetailsMapper extends Mapper<Object,Text,Text,Text> {
	
	@Override
	public void map(Object key,Text value,Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		String dept = tokens[8];
		String salary = tokens[9];
		
		context.write(new Text(dept+"-"+salary), value);
		
		
		
		
		
	}
	
	

}
