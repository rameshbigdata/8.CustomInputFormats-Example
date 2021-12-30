package com.ramesh.CTF;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SfaxInputFormat extends FileInputFormat<CustomDate,CityTemperature> {

	@Override
    public RecordReader<CustomDate,CityTemperature> createRecordReader(InputSplit inputSplit,TaskAttemptContext context)
        throws IOException, InterruptedException {

		return new SfaxRecordReader(); 
    }
}

 