package KPI_4;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class projectCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	
	@Override
	public void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException,InterruptedException
	{
		long sum=0;
		for(LongWritable val:values)
		{
			sum+=val.get();
		}
		
		context.write(new Text(key), new LongWritable(sum));
		
		
		
	}
	
}
