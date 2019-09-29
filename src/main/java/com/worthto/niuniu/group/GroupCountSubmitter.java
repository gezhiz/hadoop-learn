package com.worthto.niuniu.group;

import com.worthto.niuniu.common.JobProvider;
import com.worthto.niuniu.common.JobProviderTemplate;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author gezz
 * @description todo
 * @date 2019/9/27.
 */
public class GroupCountSubmitter extends JobProviderTemplate implements JobProvider {

    public static void main(String[] args) {
        Job job = new GroupCountSubmitter().getJob();

        job.setMapperClass(GroupCountMapper.class);
        job.setReducerClass(GroupCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(GroupBeanWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GroupBeanWritable.class);
        //设置分区逻辑
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(ProvinceMap.getProvinceNums());
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
        paths[0] = new Path("hdfs://master:9000/hdfs-file/group/input");
        return paths;
    }

    @Override
    public Path getOutputPath() {
        return new Path("hdfs://master:9000/hdfs-file/group/output/");
    }

}
