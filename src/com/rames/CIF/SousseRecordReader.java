package com.ramesh.CTF;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class SousseRecordReader extends RecordReader<CustomDate,CityTemperature> {
    LineRecordReader lineRecordReader;
    CustomDate key;
    CityTemperature value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader = new LineRecordReader();
        lineRecordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!lineRecordReader.nextKeyValue()) {
            return false;
        }

        String line = lineRecordReader.getCurrentValue().toString();
        String[] keyValue = line.split("\t");
        String[] keyFields = keyValue[0].split(" ");
        String[] valueFields = keyValue[1].split(" ");

        key = new CustomDate(valueFields[0],valueFields[1],valueFields[2]);
        value = new CityTemperature(keyFields[0],keyFields[1]);
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