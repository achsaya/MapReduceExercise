package com.partitioner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionerDriver extends Configured implements Tool {

	  @Override
	  public int run(String[] args) throws Exception {

	    JobConf conf = new JobConf(getConf(), PartitionerDriver.class);
	    conf.setJobName(this.getClass().getName());

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    conf.setMapperClass(PartitionerMapper.class);
	    conf.setReducerClass(PartitionerReducer.class);

	    conf.setMapOutputKeyClass(Text.class);
	    conf.setMapOutputValueClass(Text.class);

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(IntWritable.class);
	    
	    conf.setNumReduceTasks(12);
	    conf.setPartitionerClass(MyCustomPartitioner.class);

	    JobClient.runJob(conf);
	    return 0;
	  }

	  public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new PartitionerDriver(), args);
	    System.exit(exitCode);
	  }
}

