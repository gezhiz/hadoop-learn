package com.worthto.niuniu.common;

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
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public abstract class JobProviderTemplate implements JobProvider {
    private FileSystem fileSystem = null;

    @Override
    public Job getJob() {
        //设置hadoop用户名
        System.setProperty("HADOOP_USER_NAME","root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000/");
        //在本地跑不能有mapreduce.framework.name参数的配置，否则跑不起来
//        conf.set("mapreduce.framework.name", "yarn");
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
        job.setJarByClass(JobProviderTemplate.class);
        //设置reduce的数量n，将会生成n个结果文件
        //按照hash值去模（%）选择reduce task
        job.setNumReduceTasks(1);
        Path outputPath = getOutputPath();
        try {
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            //注意：下面两个路径是hdfs的路径
            FileInputFormat.setInputPaths(job, getInputPaths());
            FileOutputFormat.setOutputPath(job, outputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return job;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }


    /**
     * 配置文件输入
     * @return
     */
    public abstract Path[] getInputPaths();


    /**
     * 配置结果文件输出位置
     * @return
     */
    public abstract Path getOutputPath();
}
