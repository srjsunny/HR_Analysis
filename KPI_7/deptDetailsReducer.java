package KPI_7;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class deptDetailsReducer extends Reducer<Text,Text,Text,Text>{

    @Override
    public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
    {
    	long count=0;
        float a = 0.0f;
        long b=0;
        long c=0;
    	
    	for(Text val:values)
    	{
    	count++;
    	String tokens [] = val.toString().split(",");	
    	 a+=Float.parseFloat(tokens[0]);	
    	 b+=Long.parseLong(tokens[3]);	
    	 c+=Long.parseLong(tokens[6]); 	
    	}
    	
    float avg_satisfaction = a/count;
    long avg_monthlyHours = b/count;
    
    
    context.write(key, new Text(String.valueOf(avg_satisfaction) +"\t"+String.valueOf(avg_monthlyHours)+"\t"+String.valueOf(c)));	
    	        //dept                  // satisfaction                //monthly hours                      // no. of emp left
        	
    	 
    	
    }
	
	
}
