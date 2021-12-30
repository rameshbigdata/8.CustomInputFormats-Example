package com.ramesh.CTF;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

 
public class SfaxRecordReader  extends RecordReader<CustomDate,CityTemperature>

{
	 
    // It’s a builtin class that split each file line by line
    LineRecordReader lineRecordReader;
    CustomDate key;
    CityTemperature value;
    
	@Override
	public void initialize(InputSplit inputSplit,
			org.apache.hadoop.mapreduce.TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
		
	}
	
	
    

    // It’s the method that will be used to transform the line into key value
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }
        String line = lineRecordReader.getCurrentValue().toString();
        String[] keyValue = line.split("\t");
        String[] keyFields = keyValue[0].split(" ");
        String[] valueFields = keyValue[1].split(" ");

          key = new CustomDate(keyFields[0],keyFields[1],keyFields[2]);
          value = new CityTemperature(valueFields[0],valueFields[1]);
        return true;
    }

    @Override
    public CustomDate getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public CityTemperature getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }


}