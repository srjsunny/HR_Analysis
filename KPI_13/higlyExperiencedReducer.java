package KPI_13;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class higlyExperiencedReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	@Override
	public void reduce(Text key,Iterable<IntWritable>values,Context context)throws IOException,InterruptedException
	{
		
		// key (dept)                          values (experience)
		//sales                                [3,5,8....]
      
		 int ans=-1;
		
		
		for(IntWritable val:values)
		{
			if(val.get()>ans)
			{
				ans=val.get();
			}
			
			
		}
	
		
	context.write(key, new IntWritable(ans));	
		
		
	}

}
