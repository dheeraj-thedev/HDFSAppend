package examples;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;


public class Launcher {

    public static void main(String[] args) throws IOException {

        HdfsAppendExample example = new HdfsAppendExample();
        String coreSite = "/opt/hadoop/etc/hadoop/core-site.xml";
        String hdfsSite = "/opt/hadoop/etc/hadoop/hdfs-site.xml";
        FileSystem fileSystem = example.configureFileSystem(coreSite, hdfsSite);
        Path hdfsFilePath = new Path("hdfs://localhost:54310/data.csv");
        example.appendToFile(fileSystem, "How are you? \n My name is Simar \n Intern At Knoldus12", hdfsFilePath);
        example.readFromHDFS(fileSystem, "hdfs://localhost:54310/data.csv", "./target/copyFromHDFS.csv");
        example.closeFileSystem(fileSystem);
    }
}
