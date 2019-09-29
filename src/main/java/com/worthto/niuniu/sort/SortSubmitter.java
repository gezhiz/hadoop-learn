package com.worthto.niuniu.sort;

import com.worthto.niuniu.common.JobProvider;
import com.worthto.niuniu.common.JobProviderTemplate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class SortSubmitter extends JobProviderTemplate implements JobProvider {
    @Override
    public Path[] getInputPaths() {
        Path[] paths = new Path[1];
        paths[0] = new Path("hdfs://master:9000/hdfs-file/sort/input/");
        return paths;
    }

    @Override
    public Path getOutputPath() {
        return new Path("hdfs://master:9000/hdfs-file/sort/output/");
    }

    public static void main(String[] args) {
        Job job = new SortSubmitter().getJob();
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(PageCount.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(PageCount.class);
        job.setMapOutputValueClass(IntWritable.class);
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
