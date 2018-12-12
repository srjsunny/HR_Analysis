package KPI_7;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class deptDetailsMapper extends Mapper<Object,Text,Text,Text>{
	
	
		@Override
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException
		{
			String tokens[]= value.toString().split(",");
			String dept = tokens[8];
			
			context.write(new Text(dept),value);
			
			
			
			
		}
		
		
		
		
	}


