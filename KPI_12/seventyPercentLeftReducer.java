package KPI_12;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class seventyPercentLeftReducer extends Reducer<Text,IntWritable,Text,NullWritable> {
	
	@Override
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException
    {
	//	key              values
	 //sales           [1,0,1,1,1,0,0....]	
		
		float total_dept_strength=0;
		int count=0;
		
		for(IntWritable val:values)
		{
			total_dept_strength++;
			
		   	if(val.get() == 1)
		   	{
		   		count++;
		   	}
			
		}
		
		float percentLeft = (count/total_dept_strength)*100;
		
		if(percentLeft>70)
		{
			context.write(key, NullWritable.get());
		}
		
		
		
		
		
		
	}
	

}
