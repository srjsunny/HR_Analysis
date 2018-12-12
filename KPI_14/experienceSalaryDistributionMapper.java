package KPI_14;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class experienceSalaryDistributionMapper extends Mapper<LongWritable,Text,Text,Text> {

	//the highest exp. in company is 10 , calculated from KPI_13
	public static long highest_exp =10;
	
	@Override
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
	{
	  String []tokens = value.toString().split(",");
	  String dept = tokens[8];
	  String salary = tokens[9];
	  long exp = Long.parseLong(tokens[4]);
	  
	  if(highest_exp == exp) 
	  {
	  
		  context.write(new Text(dept+"\t"+exp), new Text(salary));
	  
	  }
	
}
}