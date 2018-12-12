package KPI_9;

import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class myReducer extends Reducer<Text,Text,Text,Text> {
	
	@Override
	public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
	{
		float a = 0.0f;
		long b = 0;		
		long count = 0;
		
		float total_num=0;
	    long isleft;
		
	    for(Text val:values)
		{
		 String []tokens = val.toString().split(",");
		 String salary = tokens[9];
		 int promoted = Integer.parseInt(tokens[7]);
         
		 if(salary.equals("low") && promoted ==0)
		 {
			 total_num++;
			a+=Float.parseFloat(tokens[0]);
			b+=Integer.parseInt(tokens[3]);
			
			isleft = Integer.parseInt(tokens[6]);
			
			if(isleft == 1)
			{
			
				count++;
				
			}
			 
			}
			
		}
		
	    float avg_satisfaction = a/total_num;
	    float avg_working = b/total_num;
	    long empLeft = count;
	    
	    context.write(key, new Text( String.valueOf(avg_satisfaction)+" "+String.valueOf(avg_working) +" "+String.valueOf(empLeft)) ) ;
	          
	                // dept            average satisfaction                average working hours          no. of emp. who left the company
	                // sales              0.52                                       25                             5
	    
	    
	    
	}
	

}
