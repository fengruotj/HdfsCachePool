package com.hdfsbuffer2.main;

import com.hdfsbuffer2.util.HdfsOperationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.Timestamp;

/**
 * locate com.hdfsbuffer
 * Created by 79875 on 2017/4/19.
 * FSDataInputStream 读取数据 基准测试
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.FSDataInputStreamBenchMark /user/root/flinkwordcount/input/resultTweets.txt
 * 2017-04-24 14:53:49  [ main:0 ] - [ WARN ]  Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
 * 2017-04-24 14:54:23  [ main:33802 ] - [ INFO ]  startTime:2017-04-24 14:53:50.964
 * 2017-04-24 14:54:23  [ main:33802 ] - [ INFO ]  endTime:2017-04-24 14:54:23.734
 * 2017-04-24 14:54:23  [ main:33802 ] - [ INFO ]  totalTime:32 s------or------0 min
 */
public class FSDataInputStreamBenchMark {
    private static final Log LOG = LogFactory.getLog(FSDataInputStreamBenchMark.class);
    private static HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
    private static byte[] recordDelimiterBytes;//记录分割符
    private static long Totalrows=0L;
    public static void main(String[] args) throws IOException {
        String inputFile=args[0];
        FileSystem fs = HdfsOperationUtil.getFs();
        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
        long startTimeSystemTime= System.currentTimeMillis();
        byte[] buf=new byte[4096];
        while (dataInputStream.read(buf,0,4096)!=-1){
            //Totalrows++;
            //kafkaUtil.publishOrderMessage(topic,partitionsNum,(int)Totalrows,text.toString());
        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
