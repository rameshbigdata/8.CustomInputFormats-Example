package com.ramesh.CTF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class CustomDate implements WritableComparable<CustomDate>{
    IntWritable year;
    IntWritable month;
    IntWritable day;

    public CustomDate() {
        year = new IntWritable();
        month = new IntWritable();
        day = new IntWritable();
    }

    public CustomDate(String year, String month, String day) {
        this.year = new IntWritable(Integer.parseInt(year));
        this.month = new IntWritable(Integer.parseInt(month));
        this.day = new IntWritable(Integer.parseInt(day));
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year = year;
    }

    public IntWritable getMonth() {
        return month;
    }

    public void setMonth(IntWritable month) {
        this.month = month;
    }

    public IntWritable getDay() {
        return day;
    }

    public void setDay(IntWritable day) {
        this.day = day;
    }

    @Override
    public int hashCode() {
        return year.hashCode() + month.hashCode() + day.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomDate) {
            CustomDate dateWritable = (CustomDate) obj;
            return this.getYear().equals(dateWritable.getYear()) && this.getMonth().equals(dateWritable.getMonth()) && this.getDay().equals(dateWritable.getDay());
        }
        return false;
    }

    @Override
    public String toString() {
        return day.toString()+"/"+month.toString()+"/"+year.toString();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        day.readFields(dataInput);
        month.readFields(dataInput);
        year.readFields(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        day.write(dataOutput);
        month.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public int compareTo(CustomDate dateWritable) {
        int yearCmp = year.compareTo(dateWritable.getYear());
        if (yearCmp != 0) {
            return yearCmp;
        }
        int monthCmp = month.compareTo(dateWritable.getMonth());
        if (monthCmp != 0) {
            return monthCmp;
        }
        return day.compareTo(dateWritable.getDay());
    }
}
