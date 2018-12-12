package KPI_5;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class salaryDistributionReducer extends Reducer<Text,Text,Text,Text> {
	
	@Override
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
	{
		
	// key                     values	
	// sales                [low,low,low,medium,high,medium .....]
		
		
		int a =0;
		int b =0;
		int c =0;
		for(Text val:values)
		{
			if(val.toString().equals("low"))
				a++;
			
			else if(val.toString().equals("medium"))
				b++;
			
			else if(val.toString().equals("high"))
				c++;
		}
		
		String low = String.valueOf(a);
		String medium = String.valueOf(b);
		String high = String.valueOf(c);
		
		
	context.write(key,new Text(low+" "+medium+" "+high));
		
		
	}

}
