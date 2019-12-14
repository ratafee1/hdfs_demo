package cn.itcast.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsApiDemo {

//    实现文件的上传
    @Test
    public void uploadFile() throws URISyntaxException, IOException {
        final FileSystem fileSystem = FileSystem.get(new URI( "hdfs://node01:8020"), new Configuration());
//  调用方法,实现上传
        fileSystem.copyFromLocalFile(new Path("D:\\set.xml"), new Path("/"));
//        关闭filesystem
        fileSystem.close();
    }

//    实现文件的下载方式2

    @Test
    public void downloadFile2() throws IOException, URISyntaxException {
        final FileSystem fileSystem = FileSystem.get(new URI( "hdfs://node01:8020"), new Configuration());
        fileSystem.copyToLocalFile(new Path("/a.txt"),new Path("D:\\a2.txt"));
//        关闭filesystem
        fileSystem.close();
    }


//    实现文件下载
    @Test
    public void downloadFile() throws URISyntaxException, IOException {
//        获取filesystem
        final FileSystem fileSystem = FileSystem.get(new URI( "hdfs://node01:8020"), new Configuration());
//        获取hdfs的输入流
        final FSDataInputStream in = fileSystem.open(new Path( "/a.txt"));
//        获取本地路径的输出流
        final FileOutputStream fileOutputStream = new FileOutputStream("D:\\a.txt");

//        文件的拷贝
        IOUtils.copy(in,fileOutputStream);
//        关闭流
        IOUtils.closeQuietly(in);
        IOUtils.closeQuietly(fileOutputStream);
        fileSystem.close();

    }


//    hdfs创建文件夹
    @Test
    public  void  mkdirTest() throws IOException, URISyntaxException {
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
//        创建文件夹
//        final FsPermission fsPermission = new FsPermission("777");
        final boolean bl = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        fileSystem.create(new Path("/aaa2/bbb/ccc/a.txt"));
        System.out.println(bl);
        fileSystem.close();
    }

//    使用API遍历 hdfs文件
    @Test
    public void listFiles() throws IOException, URISyntaxException {
//        获取filesystem实例
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());

//        调用listfiles获取目录下所有的文件信息
        final RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
//        遍历迭代器
        while (locatedFileStatusRemoteIterator.hasNext()){
            final LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
//            获取文件的绝对路径
            System.out.println( fileStatus.getPath() + " --------" + fileStatus.getPath().getName());
//            文件的block信息
            final BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println("block number:" + blockLocations.length);


        }
    }


// 方法四
    @Test
    public  void getFileSystem4() throws IOException, URISyntaxException {
//        1.创建Configuration对象
//        2.设置文件系统的类型
//        3.获取指定的文件系统
        final FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), new Configuration());

        System.out.println(fileSystem);
    }


//  方式三
    @Test
    public  void getFileSystem3() throws IOException {
//        1.创建Configuration对象
//        2.设置文件系统的类型
//        3.获取指定的文件系统
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");

        final FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem);
    }


//    获取FileSystem：方式二
    @Test
    public void getFileSystem2() throws Exception {
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println(fileSystem);
    }



//    获取FileSystem：方式一
    @Test
    public  void getFileSystem1() throws IOException {
//        1.创建Configuration对象
//        2.设置文件系统的类型
//        3.获取指定的文件系统
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020");

        final FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem);
    }



    @Test
    public void urlHdfs() throws Exception {
        /**
         * 1.注册url
         */
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
//        获取hdfs文件的输入流
        final InputStream inputStream = new URL("hdfs://node01:8020/a.txt").openStream();
//        获取本地文件的输出流
        final FileOutputStream fileOutputStream = new FileOutputStream(new File("D:\\hello2.txt"));
//        实现复制
        IOUtils.copy(inputStream,fileOutputStream);
//        关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);

    }
}
