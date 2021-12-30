package com.ramesh.CTF;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SousseInputFormat extends FileInputFormat<CustomDate,CityTemperature> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {

        SousseRecordReader sousseRecordReader = new SousseRecordReader();
        sousseRecordReader.initialize(inputSplit, context);
        return sousseRecordReader;
    }
}
