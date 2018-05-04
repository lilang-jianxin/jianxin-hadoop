package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * @description: 数据去重
 * @author: jianxin
 * @create: 2018-05-04 08:33
 **/
public class Dedup extends Configured implements Tool {
    //行号当做key，需要用整行存储 行内有重复不能去掉 行间重复去掉
    public static class DedupMap extends Mapper<LongWritable,Text,Text,Text>{
        private  static Text line=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            line=value;
            context.write(line,new Text(""));
        }
    }
    public static  class  DedupReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Path pathin=new Path(args[0]);
        FileInputFormat.addInputPath(job,pathin);
        job.setMapperClass(DedupMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(DedupReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path pathout=new Path(args[1]);
        FileSystem fileSystem=pathout.getFileSystem(configuration);
        if (fileSystem.exists(pathout)){
            fileSystem.delete(pathout,true);
        }
        FileOutputFormat.setOutputPath(job,pathout);
        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) {
        Configuration configuration=new Configuration();
        int status=0;
        try {
            status= ToolRunner.run(configuration,new Dedup(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }
}