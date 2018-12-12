package KPI_11;

import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class experienceWiseEmpLeftReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

	@Override
	public void reduce(IntWritable key,Iterable<IntWritable>values,Context context)throws IOException,InterruptedException
	{
		
		// key                   value
		 
	//	3                        [1,0,0,0,1,1,1...]
		
      int count =0;	
		
	for(IntWritable val:values)
	{
	
		int temp = val.get();
		if(temp==1)
		{
			count++;
		}
		
		
		
	}
				
		
		
		
	context.write(key, new IntWritable(count));	
		
		
		
		
		
		
	}
	
	
	
	
	
}
