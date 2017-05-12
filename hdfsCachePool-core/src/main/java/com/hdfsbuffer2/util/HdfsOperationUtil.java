package com.hdfsbuffer2.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by 79875 on 2017/3/18.
 * HDFS 文件系统操作类
 */
public class HdfsOperationUtil {

    private static Configuration conf = new Configuration();
    private static final String HADOOP_URL="hdfs://192.168.223.202:9000";

    private static FileSystem fs;

    private static DistributedFileSystem hdfs;

    static {
        try {
            FileSystem.setDefaultUri(conf, HADOOP_URL);
            fs = FileSystem.get(conf);
            hdfs = (DistributedFileSystem)fs;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static FileSystem getFs() {
        return fs;
    }

    public static Configuration getConf() {
        return conf;
    }

    public static void setConf(Configuration conf) {
        HdfsOperationUtil.conf = conf;
    }

    /**
     * 列出所有DataNode的名字信息
     */
    public List<String> listDataNodeInfo() {
        List<String> list=new ArrayList<>();

        try {
            DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
            String[] names = new String[dataNodeStats.length];
            System.out.println("List of all the datanode in the HDFS cluster:");

            for (int i=0;i<names.length;i++) {
                names[i] = dataNodeStats[i].getHostName();
                list.add(names[i]);
            }
            System.out.println(hdfs.getUri().toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 创建文件夹可以递归创建文件夹
     * @param dir
     * @throws IOException
     */
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        hdfs.mkdirs(path);
        System.out.println("new dir \t" + conf.get("fs.default.name") + dir);
    }

    /**
     * 列出当前目录下所有文件
     * @param dirName
     * @throws IOException
     */
    public void listFiles(String dirName) throws IOException {
        Path f = new Path(dirName);
        System.out.println(dirName + " has all files:");
        FileStatus[] status = hdfs.listStatus(f);
        for (int i = 0; i< status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }
    }

    /**
     *  删除文件
     * @param fileName
     * @throws IOException
     */
    public void deleteFile(String fileName) throws IOException {
        Path f = new Path(fileName);
        boolean isExists = hdfs.exists(f);
        if (isExists) { //if exists, delete
            boolean isDel = hdfs.delete(f,true);
            System.out.println(fileName + "  delete? \t" + isDel);
        } else {
            System.out.println(fileName + "  exist? \t" + isExists);
        }
    }

    /**
     * 查看文件是否存在
     */
    public boolean checkFileExist(String filePath) {
        boolean exist=false;
        try {
            Path a= hdfs.getHomeDirectory();
            System.out.println("main path:"+a.toString());

            //Path f = new Path("/user/xxx/input01/");
            Path f = new Path(filePath);
            exist = fs.exists(f);
            System.out.println("Whether exist of this file:"+exist);

            //删除文件
//          if (exist) {
//              boolean isDeleted = hdfs.delete(f, false);
//              if(isDeleted) {
//                  System.out.println("Delete success");
//              }
//          }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exist;
    }

    /**
     *创建文件到HDFS系统上
     */
    public void createFile(String fileLocation,String text) {
        try {
            //Path f = new Path("/user/xxx/input02/file01");
            Path f = new Path(fileLocation);
            System.out.println("Create and Write :"+f.getName()+" to hdfs");

            FSDataOutputStream os = fs.create(f, true);
            Writer out = new OutputStreamWriter(os, "utf-8");//以UTF-8格式写入文件，不乱码
            out.write(text);
            out.close();
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 读取本地文件到HDFS系统<br>
     * 请保证文件格式一直是UTF-8，从本地->HDFS
     */
    public void copyFileToHDFS(String SouceFilePath,String distinctFilePath) {
        try {
            Path f = new Path(distinctFilePath);
            File file = new File(SouceFilePath);

            FileInputStream is = new FileInputStream(file);
            InputStreamReader isr = new InputStreamReader(is, "utf-8");
            BufferedReader br = new BufferedReader(isr);

            FSDataOutputStream os = fs.create(f, true);
            Writer out = new OutputStreamWriter(os, "utf-8");

            String str = "";
            while((str=br.readLine()) != null) {
                out.write(str+"\n");
            }
            br.close();
            isr.close();
            is.close();
            out.close();
            os.close();
            System.out.println("Write content of file "+file.getName()+" to hdfs file "+f.getName()+" success");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 复制文件到HDFS上 hdfs上保存数据块
     * @param localFile 本地文件地址
     * @param hdfsFile hdfs文件位置
     * @param buffersize 每一个数据块的大小
     * @throws IOException
     */
    public void copyFileToHDFSBlock(String localFile, String hdfsFile,String buffersize) throws IOException{
        conf.set("dfs.block.size", buffersize);//第二个参数的单位是字节，并且是字符串形式
        FileSystem fs=FileSystem.get(conf);
        Path src=new Path(localFile);//参数是本地文件的绝对路径的字符串形式
        Path dst=new Path(hdfsFile);
        fs.copyFromLocalFile(src,dst);
        System.out.println("upload to:"+conf.get("fs.default.name"));
    }

    /**
     * 取得文件块所在的位置..
     */
    public List<String> getBolockHosts(String filePath) {
        List<String> list=new ArrayList<>();
        try {
            Path f = new Path(filePath);
            FileStatus fileStatus = fs.getFileStatus(f);

            BlockLocation[] blkLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for (BlockLocation currentLocation : blkLocations) {
                String[] hosts = currentLocation.getHosts();
                for (String host : hosts) {
                    list.add(host);
                    System.out.println(host);
                }
            }

            //取得最后修改时间
            long modifyTime = fileStatus.getModificationTime();
            Date d = new Date(modifyTime);
            System.out.println(d);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 读取hdfs中的文件内容
     */
    public void readFileFromHdfs(String filePath) {
        try {
            Path f = new Path(filePath);

            FSDataInputStream dis = fs.open(f);
            InputStreamReader isr = new InputStreamReader(dis, "utf-8");
            BufferedReader br = new BufferedReader(isr);
            String str = "";
            while ((str = br.readLine()) !=null) {
                System.out.println(str);
            }
            br.close();
            isr.close();
            dis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * list all file/directory
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    public List<FileStatus> listFileStatus(String path) throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus fileStatus[]=fs.listStatus(new Path(path));
        List list=new ArrayList();
        int listlength=fileStatus.length;
        for (int i=0 ;i<listlength ;i++){
            if (fileStatus[i].isDirectory() == false) {
                System.out.println("filename:"
                        + fileStatus[i].getPath().getName() + "\tsize:"
                        + fileStatus[i].getLen());
            }
            list.add(fileStatus[i]);
        }
        return list;
    }

    /**
     * 列出输入文件目录下所有文件大小 不包括子文件
     * @param path 输入文件路径
     * @return
     * @throws IOException
     */
    public long getInputDirectoryLength(String path) throws IOException {
        FileStatus fileStatus[]=fs.listStatus(new Path(path));
        long length=0L;
        for (int i=0 ;i<fileStatus.length ;i++){
            FileStatus fileStatu = fileStatus[i];
            if (fileStatu.isDirectory() == false) {
                length+=fileStatu.getLen();
            }
        }
        return length;
    }

}

