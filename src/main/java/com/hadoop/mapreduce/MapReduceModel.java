package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @program: jianxin-hadoop
 * @description: MapReduce编程模板
 * @author: jianxin
 * @create: 2018-05-02 10:49
 **/
public class MapReduceModel extends Configured implements Tool {


  public static class ModelMapper extends Mapper<LongWritable ,Text,Text,IntWritable>{
       @Override
       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           super.map(key, value, context);
      //电饭锅

       }
   }
  public static class ModelReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
      @Override
      protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          super.reduce(key, values, context);

      }
  }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        Job job=Job.getInstance(conf,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Path inpath=new Path(args[0]);
        FileInputFormat.addInputPath(job,inpath);
        job.setMapperClass(ModelMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(ModelReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path outpath=new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outpath);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        Configuration configuration=new Configuration();
        //snappy压缩设置
	    configuration.set("mapreduce.map.output.compress", "true");
	    configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        int status= 1;
        try {
            status = ToolRunner.run(configuration,new MapReduceModel(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }
}