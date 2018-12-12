package KPI_2;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class empRemainingReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException
	{
		int count =0;
		for(IntWritable val: values)
		{
			if(val.get() == 0)
				count++;
		}
		
	context.write(key,new IntWritable(count));
	}
}
