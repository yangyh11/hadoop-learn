package com.yangyh.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @description: 调用hdfs客户端api
 * @author: yangyh
 * @create: 2019-11-01 14:26
 */
public class HdfsTest {

    private Configuration configuration;
    private FileSystem fileSystem;

    @Before
    public void before() throws IOException {
        configuration = new Configuration(true);
        configuration.set("dfs.blocksize", "1048576");
        fileSystem = FileSystem.get(configuration);

    }

    @After
    public void after() throws IOException {
        fileSystem.close();

    }


    @Test
    public void testLs() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/opt"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }
    }

    @Test
    public void testAddDir() throws IOException {
        boolean flag = fileSystem.mkdirs(new Path("/usr/local"));
        System.out.println(flag ? "创建成功" : "失败");
    }

    @Test
    public void testDelDir() throws IOException {
        //
//        boolean flag = fileSystem.delete(new Path("/usr"));
        //boolean recursive 是否递归删除
        boolean flag = fileSystem.delete(new Path("/opt/hello2.txt"),false);
        System.out.println(flag ? "删除成功" : "失败");
    }


    @Test
    public void uploadFIle() throws IOException {
        FSDataOutputStream fsdos = fileSystem.create(new Path("/opt/hello3.txt"));
        FileInputStream fis = new FileInputStream("E:\\hello.txt");

        byte[] buf = new byte[1024];
        int len = -1;

        // 从buf读字节len个，因为读取了len个 TODO
        while ((len = fis.read(buf)) != -1) {
            // 写到HDFS
            fsdos.write(buf, 0, len);
        }

        fis.close();
        fsdos.flush();
        fsdos.close();
    }

    @Test
    public void uploadFIle2() throws IOException {
        FSDataOutputStream fsdos = fileSystem.create(new Path("/opt/hello4.txt"));
        FileInputStream fis = new FileInputStream("E:\\hello.txt");

        IOUtils.copyBytes(fis,fsdos,configuration);

        fis.close();
        fsdos.flush();
        fsdos.close();
    }

    @Test
    public void downLoad() throws IOException {
        FSDataInputStream fsInputStream = fileSystem.open(new Path("/opt/hello4.txt"));
        FileOutputStream outputStream = new FileOutputStream("D:\\hello.txt");

        IOUtils.copyBytes(fsInputStream, outputStream, configuration);

        fsInputStream.close();
        outputStream.flush();
        outputStream.close();
    }

    @Test
    public void getBlockLocation() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/opt/hello.txt"));
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation blockLocation : fileBlockLocations) {
            System.out.println(blockLocation);
        }
    }

    @Test
    public void readLocalData() throws IOException {
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/opt/hello.txt"));
        fsDataInputStream.seek(0);
        System.out.println(fsDataInputStream.readLine());

        fsDataInputStream.seek(100);
        System.out.println(fsDataInputStream.readLine());
    }








}
