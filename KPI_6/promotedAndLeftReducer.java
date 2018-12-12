package KPI_6;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class promotedAndLeftReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	
	@Override
	public void reduce(Text key,Iterable<LongWritable> values,Context context)throws IOException,InterruptedException
	{
		// key                values
		//sales             [0,1,1,1,1,0,2,2,2,2,.....]
         
		long count=0;
		
		
	    for(LongWritable val:values)
	    {
	    if(val.get()==2)
	    	count++;
	    }
		
	context.write(key, new LongWritable(count));	
		
		
	}
	
	
	
}
