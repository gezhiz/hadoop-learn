package com.worthto.niuniu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

/**
 * 用于提交map reduce 任务的客户端程序
 * 1.封装job运行时必要的参数
 * 2、与服务端交互，提交任务
 * @author gezz
 * @description todo
 * @date 2019/9/25.
 */
public class JobSubmitter {

    private static FileSystem fileSystem = null;

    public static void main(String[] args) {
        //设置hadoop用户名
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000/");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        //设置jar工作目录
        conf.set("mapreduce.job.jar", "/Users/gezz/IdeaProjects/hadoop-learn/out/artifacts/hadoop_learn_jar/hadoop-learn.jar");
        //配置跨平台运行
        conf.set("mapreduce.app-submission.cross-platform", "true");
        Job job = null;
        try {
            fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), conf, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        try {
            job = Job.getInstance(conf,"word count java");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setReducerClass(WordCountReduce.class);
        job.setMapperClass(WordCountMap.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setJarByClass(JobSubmitter.class);
        job.setNumReduceTasks(1);
        Path outputPath = new Path("hdfs://master:9000/hdfs-file/wordcount/output/");
        try {
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            //注意：下面两个路径是hdfs的路径
            FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/hdfs-file/wordcount/input"));
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            job.submit();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        //是否打印日志
        boolean verbose = true;
        try {
            boolean result = job.waitForCompletion(verbose);
            if (result) {
                System.out.println("成功运行任务");
            }
            System.out.println("结束运行");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable ONE = new LongWritable(1);
        private Text word = new Text();

        /**
         *
         * @param key 每个数据的记录在数据分片中的字节偏移量，数据类型是LongWritable
         * @param value 每行的内容
         * @throws IOException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                try {
                    context.write(word, ONE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static class WordCountReduce extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for(LongWritable value: values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
}
