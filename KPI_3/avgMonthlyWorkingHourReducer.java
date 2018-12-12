package KPI_3;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class avgMonthlyWorkingHourReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
	@Override
	public  void reduce(Text key,Iterable<LongWritable> values,Context context)throws IOException,InterruptedException
	{
		long count=0,sum=0;
		long avg;
		for(LongWritable val:values)
		{
			sum+=val.get();
			count++;
		}
		avg = sum/count;

		context.write(new Text(key),new LongWritable(avg));
	}
}
