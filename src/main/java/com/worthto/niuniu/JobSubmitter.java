package com.worthto.niuniu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 用于提交map reduce 任务的客户端程序
 * 1.封装job运行时必要的参数
 * 2、与服务端交互，提交任务
 * @author gezz
 * @description todo
 * @date 2019/9/25.
 *
 *
 *
 * 在集群上运行MapReduce 的方法：
 * 1、在编译器打包
 * Build----Build Artifacts----
 * 2、上传到服务器
 * scp /Users/gezz/IdeaProjects/hadoop-learn/out/artifacts/wordCount/wordCount.jar root@master:/jar
 * 3、在服务器上运行
 *  hadoop jar wordCount.jar com.worthto.niuniu.JobSubmitter
 */
public class JobSubmitter {

    private static FileSystem fileSystem = null;

    public static void main(String[] args) {
        //设置hadoop用户名
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000/");
        //在本地跑不能有mapreduce.framework.name参数的配置，否则跑不起来
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "master");
        //设置jar工作目录
//        conf.set("mapreduce.job.jar", "/Users/gezz/IdeaProjects/hadoop-learn/out/artifacts/wordCount/wordCount.jar");
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
            //0就是true
            System.exit(result ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
