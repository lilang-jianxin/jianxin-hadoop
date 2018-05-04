package com.hadoop.mapreduce;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @program: jianxin-hadoop
 * @description: 数据排序
 * @author: jianxin
 * @create: 2018-05-04 10:09
 **/
public class SortData extends Configured implements Tool {

    public static class SortMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
        private IntWritable data=new IntWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value==null){
                return;
            }
            data.set(Integer.parseInt(value.toString()));
            context.write(data,new IntWritable(1));
        }
    }
    public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        public    static  IntWritable  data_index=new IntWritable(1);
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable data:values){
                context.write(data_index,key);
                data_index=new IntWritable(data_index.get()+1);

            }
        }
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration=getConf();
        Job job=Job.getInstance(configuration,this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        Path pathin=new Path(args[0]);
        FileInputFormat.addInputPath(job,pathin);
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
            status= ToolRunner.run(configuration,new SortData(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }
}