package com.ramesh.CTF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
 

//each value should implements Writable
public class CityTemperature implements Writable {
    //fields
    Text city;
    IntWritable temperature;

    // A constructor with no args should be present, else hadoop will throw an error
    public CityTemperature() {
        city = new Text();
        temperature = new IntWritable();
    }

    public CityTemperature(String city, String degree) {
        this.city = new Text(city);
        this.temperature = new IntWritable(Integer.parseInt(degree));
    }

    // this method will be used when deserialising data
    
    public void readFields(DataInput dataInput) throws IOException {
        city.readFields(dataInput);
        temperature.readFields(dataInput);
    }

    // this method will be used when serialising data
   
    public void write(DataOutput dataOutput) throws IOException {
        city.write(dataOutput);
        temperature.write(dataOutput);
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    @Override
    public int hashCode() {
        return city.hashCode() + temperature.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CityTemperature) {
            CityTemperature textIntWritable = (CityTemperature) obj;
            return this.getCity().equals(textIntWritable.getCity()) && this.getTemperature().equals(textIntWritable.getTemperature());
        }
        return false;
    }

    @Override
    public String toString() {
        return city.toString() + " " + temperature.toString();
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(IntWritable temperature) {
        this.temperature = temperature;
    }

}