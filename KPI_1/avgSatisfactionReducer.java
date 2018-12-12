package KPI_1;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class avgSatisfactionReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	@Override
	public void reduce(Text key, Iterable<FloatWritable> values,Context context)throws IOException,InterruptedException
	{
		int count=0;
		float sum=0.0f;
		for(FloatWritable val:values)
		{
			sum+=val.get();
			count++;
			
		}
		float avg = sum/count;
		context.write(new Text(key),new FloatWritable(avg));
		
		
	}

}
