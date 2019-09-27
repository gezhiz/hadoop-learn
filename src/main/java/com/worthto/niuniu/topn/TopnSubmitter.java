package com.worthto.niuniu.topn;

import com.worthto.niuniu.common.JobProvider;
import com.worthto.niuniu.common.JobProviderTemplate;
import com.worthto.niuniu.wordcount.WordCountMap;
import com.worthto.niuniu.wordcount.WordCountReduce;
import com.worthto.niuniu.wordcount.WordCountSubmitter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class TopnSubmitter extends JobProviderTemplate implements JobProvider {
    @Override
    public Path[] getInputPaths() {
        Path[] paths = new Path[1];
        paths[0] = new Path("hdfs://master:9000/hdfs-file/topn/input/");
        return paths;
    }

    @Override
    public Path getOutputPath() {
        return new Path("hdfs://master:9000/hdfs-file/topn/output/");
    }



    public static void main(String[] args) {
        Job job = new TopnSubmitter().getJob();
        job.setMapperClass(TopnMapper.class);
        job.setReducerClass(TopnReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
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
