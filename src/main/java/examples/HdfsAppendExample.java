package examples;

/**
 * Created by simar on 27/4/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import sun.security.pkcs11.wrapper.Constants;

import java.io.*;

//import java.nio.file.Files;
//import java.nio.file.attribute.PosixFilePermission;
//import org.apache.hadoop.fs.Path;

public class HdfsAppendExample {

    public FileSystem configureFileSystem(String coreSite, String hdfsSite) {
        FileSystem fileSystem = null;
        try {
            Configuration conf = new Configuration();
            conf.setBoolean("dfs.support.append", true);
            Path coreSitePath = new Path(coreSite);
            Path hdfsSitePath = new Path(hdfsSite);
            conf.addResource(coreSitePath);
            conf.addResource(hdfsSitePath);
            fileSystem = FileSystem.get(conf);
            return fileSystem;
        } catch (IOException ex) {
            System.out.println("Error occured while configuring FileSystem");
            return fileSystem;
        }
    }

//    public String readFromHDFS(FileSystem fileSystem, Path source) {
//
//    }

    public void appendToFile(FileSystem fileSystem, String content, Path dest) throws IOException {

        if (!fileSystem.exists(dest))
            fileSystem.createNewFile(dest);

        boolean exist = fileSystem.exists(dest);
        System.out.println("File exists : " + exist);
      //  fileSystem.setPermission(dest, FsPermission.valueOf("-rwxrwxrwx"));

        //  System.out.println("---------hdfs files--------- \n "+fileSystem.listFiles(hdfsFilePath,false).next().toString());
        Boolean flag = Boolean.valueOf(fileSystem.getConf().get("dfs.support.append"));
        System.out.println("flag append : " + flag);

        if (flag) {
            FSDataOutputStream fsout = fileSystem.create(dest, true);
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
     //   fileSystem.close();
    }

    public void readFromHDFS(FileSystem fileSystem, String hdfsStorePath, String localFilePath) {
//        try {
//            Path hdfsPath = new Path(hdfsStorePath);
//            Path localSystemPath = new Path(localFilePath);
//            fileSystem.copyToLocalFile(hdfsPath, localSystemPath);
//        }
//        catch (IOException ex){
//            System.out.println("Could not read from HDFS");
//        }
        System.out.println("ReadingFile using BufferedReader - \n");
        Path hdfsPath = new Path(hdfsStorePath);
        try{
        BufferedReader bfr=new BufferedReader(new InputStreamReader(fileSystem.open(hdfsPath)));
        String str = null;

            while ((str = bfr.readLine()) != null) {
                System.out.println(str);
            }
        }
        catch (IOException ex){
            System.out.println("----------Could not read from HDFS---------\n"+ex.getMessage());
        }
    }

    public void closeFileSystem(FileSystem fileSystem){
        try {
            fileSystem.close();
        }
        catch (IOException ex){
            System.out.println("Could not close FileSystem");
        }
    }

}
