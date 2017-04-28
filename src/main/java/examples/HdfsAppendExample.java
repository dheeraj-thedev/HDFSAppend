package examples;

/**
 * Created by simar on 27/4/17.
 */

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
//import java.nio.file.Files;
//import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
//import org.apache.hadoop.fs.Path;

public class HdfsAppendExample {

//    public FileSystem configureFileSystem(String coreSitePath, String hdfsSitePath) {
//
//    }

    public void appendToFile(String content, Path dest) throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        Path confSitePath = new Path("/opt/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSitePath = new Path("/opt/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(confSitePath);
        conf.addResource(hdfsSitePath);
        FileSystem hdfs = FileSystem.get(conf);
        Path hdfsFilePath = new Path("hdfs://localhost:54310/data.csv");
        hdfs.createNewFile(hdfsFilePath);

      //  Path workingDir = hdfs.getWorkingDirectory();
      //  System.out.println("Working directory: "+workingDir);



        boolean exist = hdfs.exists(hdfsFilePath);
        System.out.println("File exists : "+exist);
        hdfs.setPermission(hdfsFilePath, FsPermission.valueOf("-rwxrwxrwx"));
        //hdfsFilePath.getFileSystem(conf).listFiles(hdfsFilePath, false);
        System.out.println("---------hdfs files--------- \n "+hdfs.listFiles(hdfsFilePath,false).next().toString());
        Boolean flag = Boolean.valueOf(hdfs.getConf().get("dfs.support.append"));
        System.out.println("flag append : "+flag);
        System.out.println("Configuration : "+conf.toString());

        if (flag) {
           // FSDataOutputStream fsout = hdfs.append(hdfsFilePath);
            FSDataOutputStream fsout = hdfs.create(hdfsFilePath, true);
            // wrap the outputstream with a writer
            PrintWriter writer = new PrintWriter(fsout);
            writer.append(content);
         //   writer.write(content);
            fsout.hflush();

            writer.close();

            fsout.close();
        } else {
            System.err.println("please set the dfs.support.append to be true");
        }
        hdfs.close();
    }

//    public String readFromHDFS(FileSystem fileSystem, Path source) {
//
//    }



    public static void main(String args[]) throws IOException{
        HdfsAppendExample example = new HdfsAppendExample();
        File file = new File("File");
        Path path = new Path("Path");
        example.appendToFile("How are you? \n My name is Simar \n Intern At Knoldus12", path);
    }

}
