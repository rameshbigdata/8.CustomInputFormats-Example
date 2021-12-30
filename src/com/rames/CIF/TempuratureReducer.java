package com.ramesh.CTF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TempuratureReducer extends Reducer<CustomDate,IntWritable,CustomDate,IntWritable>{


    protected void reduce(CustomDate key, Iterable<IntWritable> values, Reducer.Context context)
        throws IOException, InterruptedException {

        int sum = 0;
        int nbr =0 ;
        for (IntWritable value : values) {
            nbr++;
            sum=sum+value.get();
        }

        context.write(key, new IntWritable(sum/nbr));
    }
}