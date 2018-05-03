package com.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @program: jianxin-hadoop
 * @description: 单词计数 java.lang.ClassNotFoundException: hadoop运行的时候自己编译WordCount执行报错  解决办法：ChineseWordCount增加命名空间.
                   bin/hadoop jar /home/lilang/jianxin.hadoop.wordcount.jar com.hadoop.mapreduce.CountWord  /lilang_wordcount_input/hello.txt  /lilang_wordcount_output
 * @author: jianxin
 * @create: 2018-05-02 11:49
 **/
public class CountWord extends Configured implements Tool {


    public static class TokennizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        //用于存储切下来的单词
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //控制台中看不到但是可以到hadoop安装目录下的logs/userlogs/container_****/stdout中看到
            System.out.println(value);
            if (value==null){
                return;
            }


            String lineValue = value.toString();

            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while(stringTokenizer.hasMoreTokens()){
                // get word value
                String wordValue = stringTokenizer.nextToken();
                // set value
                word.set(wordValue);;
                // output
                context.write(word, one);
            }
        }
    }

    public static class TokennizerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // sum tmp
            int sum= 0 ;
            // iterator
            for(IntWritable value: values){
                // total
                sum += value.get();
            }
            // set value
            result.set(sum);

            // output
            context.write(key, result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Path pathin=new Path(args[0]);
        FileInputFormat.addInputPath(job,pathin);

        job.setMapperClass(TokennizerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(TokennizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
        System.out.println("****************main下的输出可以直接在控制台中看到****************");
        int status=0;
        try {
            status= ToolRunner.run(configuration,new CountWord(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(status);
    }

}    