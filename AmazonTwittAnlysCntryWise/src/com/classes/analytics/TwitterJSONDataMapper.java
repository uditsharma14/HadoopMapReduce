package com.classes.analytics;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class TwitterJSONDataMapper extends
		Mapper<LongWritable, MapWritable, Text, IntWritable> {
    public static int recordCounter=0;
	@Override
	protected void map(LongWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Reading the record number: " +recordCounter);
		recordCounter++;
		for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
			//System.out.println("key :"+ entry.getKey() +" " + "Value :"+entry.getValue());
			if(("user").equalsIgnoreCase(entry.getKey().toString())){
				String valueStr=entry.getValue().toString();
				JsonParser parser = new JsonParser();
				JsonObject userOject = parser.parse(valueStr).getAsJsonObject();
			    System.out.println("value :" +userOject.get("location"));
			    JsonElement location=userOject.get("location");
			    if(null != location && !"null".equalsIgnoreCase(location.toString())){
			    context.write(new Text(String.valueOf(location)),new IntWritable(1));	
			    }
			}
		}
	}
}