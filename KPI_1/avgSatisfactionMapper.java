package KPI_1;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class avgSatisfactionMapper extends Mapper<Object,Text,Text,FloatWritable> {
	
	@Override
	public void map(Object key,Text value, Context context)throws IOException,InterruptedException
	{
		String []tokens = value.toString().split(",");
		String dept = tokens[8];
		float satisfaction_level  = Float.parseFloat(tokens[0]);
		context.write(new Text(dept),new FloatWritable(satisfaction_level));
		
		
	}

}
