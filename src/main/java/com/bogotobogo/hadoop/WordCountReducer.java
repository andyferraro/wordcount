/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bogotobogo.hadoop;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author andyferraro
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while(valuesIt.hasNext()){
			sum = sum + valuesIt.next().get();
		}
		context.write(key, new IntWritable(sum));
	}	
}
