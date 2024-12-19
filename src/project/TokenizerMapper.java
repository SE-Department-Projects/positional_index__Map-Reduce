package project;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
	// private Text word = new Text();

	
	// this method will executed with each document
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {


	    String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
	    String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);

	    String fullLine[] = value_raw.split(" ");
	    
	    String word;
	    int position;
		for(int i = 0; i < fullLine.length;i ++) {  
	
			word = fullLine[i];
			position = i + 1;
			
			context.write(new Text(word), new Text(DocId + ":" + position));
		}
	}

}
