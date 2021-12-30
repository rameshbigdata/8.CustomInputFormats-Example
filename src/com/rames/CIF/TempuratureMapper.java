package com.ramesh.CTF;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TempuratureMapper extends Mapper<CustomDate,CityTemperature ,CustomDate,IntWritable>{


    protected void map(CustomDate key, CityTemperature value, Mapper.Context context) throws IOException, InterruptedException
    
    {
        context.write(key,value.getTemperature());
    }

}