package com.worthto.niuniu.wordcount;

import com.worthto.niuniu.common.JobProvider;
import com.worthto.niuniu.common.JobProviderTemplate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

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
 *  hadoop jar wordCount.jar com.worthto.niuniu.WordCountSubmitter
 */
public class WordCountSubmitter extends JobProviderTemplate implements JobProvider {

    public static void main(String[] args) {
        Job job = new WordCountSubmitter().getJob();

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

    @Override
    public Path[] getInputPaths() {
        Path[] paths = new Path[1];
        paths[0] = new Path("hdfs://master:9000/hdfs-file/wordcount/input");
        return paths;
    }

    @Override
    public Path getOutputPath() {
        return new Path("hdfs://master:9000/hdfs-file/wordcount/output/");
    }

}
