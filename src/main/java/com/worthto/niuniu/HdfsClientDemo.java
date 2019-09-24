package com.worthto.niuniu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;

/**
 * hdfs api 客户端操作演示
 * @author gezz
 * @description todo
 * @date 2019/9/24.
 */
public class HdfsClientDemo {
    private FileSystem fileSystem = null;
    @Before
    public void before() {
        try {
            // 会从hadoop安装目录加载默认的配置： core-default hdfs-default.xml core-site.xml hdfs-site.xml 等
            Configuration configuration = new Configuration();
            //设置副本数量
            configuration.set("dfs.replication", "2");
            //设置文件的切块大小
            configuration.set("dfs.blockSize", "64m");
            //Rpc接口, 端口的配置在core-site中 "fs.defaultFS"
            fileSystem = FileSystem.get(new URI("hdfs://master:9000/"), configuration, "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
    /**
     * 从本地上传文件到hdfs
     */
    @Test
    public void testUpload() {

        Path src = new Path("/Users/gezz/2019-05-28招聘帖子列表.xls");
        Path dst = new Path("/java-test");
        try {
            fileSystem.copyFromLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从hdfs下载文件到本地磁盘
     */
    @Test
    public void testGet() {
        Path src = new Path("/java-test/2019-05-28招聘帖子列表.xls");
        Path dst = new Path("/Users/gezz/hdfs-file/");
        try {
            fileSystem.copyToLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从hdfs下载文件到本地磁盘
     */
    @Test
    public void testRename() {
        Path src = new Path("/java-test/2019-05-28招聘帖子列表1.xls");
        Path dst = new Path("/java-test/2019-05-28招聘帖子列表.xls");
        try {
            fileSystem.rename(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查询文件信息（无文件夹信息）
     */
    @Test
    public void testFind() {
        Path src = new Path("/java-test");
        RemoteIterator<LocatedFileStatus> remoteIterator = null;
        try {
            remoteIterator = fileSystem.listFiles(src, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (remoteIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = remoteIterator.next();
                System.out.println("文件长度：" + locatedFileStatus.getLen());
                System.out.println("文件路径：" + locatedFileStatus.getPath());
                System.out.println("文件块大小：" + locatedFileStatus.getBlockSize());
                for (BlockLocation blockLocation : locatedFileStatus.getBlockLocations()) {
                    for (String name: blockLocation.getNames()) {
                        System.out.println("块位置：" + name);
                    }
                    System.out.println("-----");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查询文件和文件夹信息,只看一级目录
     */
    @Test
    public void testFind1() {
        Path src = new Path("/");
        FileStatus[] fileStatuses = null;
        try {
            fileStatuses = fileSystem.listStatus(src);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (FileStatus fileStatus : fileStatuses) {
            String fileOrDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            System.out.println(fileOrDir + "长度：" + fileStatus.getLen());
            System.out.println(fileOrDir + "路径：" + fileStatus.getPath());
            System.out.println(fileOrDir + "块大小：" + fileStatus.getBlockSize());
            System.out.println("---------------------------------------------------------");
        }
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * hdfs 读取文件接口
     */
    @Test
    public void testRead() {
        String path = "/java-test/hadoop-test1.txt";
        FSDataInputStream in= null;
        try {
            in = fileSystem.open(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
                fileSystem.close();
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * hdfs 读取文件接口
     */
    @Test
    public void testWrite() {
        String sourcePath = "/Users/gezz/hdfs-file/hadoop-test1.txt";
        String path = "/java-test/hadoop-test2.txt";
        FSDataOutputStream out= null;
        try {
//            out = fileSystem.append(new Path(path));
            out = fileSystem.create(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = new byte[1024];

        BufferedWriter bufferedWriter = null;
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(sourcePath)));
            bufferedWriter = new BufferedWriter(new OutputStreamWriter(out));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        String line = null;
        try {
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bufferedWriter.close();
            bufferedReader.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
