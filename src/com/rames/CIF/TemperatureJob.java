package com.ramesh.CTF;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 


public class TemperatureJob extends Configured implements Tool {
	 
    @Override
    public int run(String[] args) throws Exception {
       
    	args = new String[] { 
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/6.InputFormats/Input_Path/sfax.txt",
				"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/6.InputFormats/Input_Path/sousse.txt",
    			"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/6.InputFormats/Output_Path/"};
				 
				/* delete the output directory before running the job */
				FileUtils.deleteDirectory(new File(args[2])); 
				 
				if (args.length != 3) {
				System.err.println("Please specify the input and output path");
				System.exit(-1);
				}
				
				System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
				

        // getConf method it's inherited from Configured class, that's why we should extends Configured
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "tp4");
        

        // We can deal with multiple inputs in that way, so we relate each path to a specific mapper and specific input format
        MultipleInputs.addInputPath(job, new Path(args[0]), SfaxInputFormat.class, TempuratureMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), SousseInputFormat.class, TempuratureMapper.class);

       

        job.setJarByClass(TemperatureJob.class);
        job.setMapperClass(TempuratureMapper.class);
        //specify the reducer
        job.setReducerClass(TempuratureReducer.class);
        
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        
        
        job.setMapOutputKeyClass(CustomDate.class);
        job.setMapOutputValueClass(IntWritable.class);

        
        //specify the output format
        job.setOutputFormatClass(TextOutputFormat.class);
        //specify the output path
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        //specify the output key that will be used as a map output key also
        job.setOutputKeyClass(CustomDate.class);
        // specify the output value that will be used as a map output value also
        job.setOutputValueClass(IntWritable.class);

        // run the job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(),
                new TemperatureJob(), args);
        System.exit(exitCode);
    }
     

}