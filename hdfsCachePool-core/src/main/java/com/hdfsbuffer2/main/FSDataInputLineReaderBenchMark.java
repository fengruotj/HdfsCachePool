package com.hdfsbuffer2.main;

import com.hdfsbuffer2.util.HdfsOperationUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;

/**
 * locate com.hdfsbuffer2.main
 * Created by 79875 on 2017/4/30.
 * FSDataInputStream 读取每一行数据 基准测试
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-core-1.0-SNAPSHOT.jar com.hdfsbuffer2.main.FSDataInputLineReaderBenchMark /user/root/flinkwordcount/input/resultTweets.txt
 * 2017-04-30 13:05:29  [ main:60068 ] - [ INFO ]  startTime:2017-04-30 13:04:30.746
 * 2017-04-30 13:05:29  [ main:60068 ] - [ INFO ]  endTime:2017-04-30 13:05:29.598
 * 2017-04-30 13:05:29  [ main:60068 ] - [ INFO ]  totalTime:58 s------or------0 min
 */
public class FSDataInputLineReaderBenchMark {
    private static final Logger LOG = LoggerFactory.getLogger(FSDataInputLineReaderBenchMark.class);
    public static void main(String[] args) throws IOException {
        String inputFile=args[0];
        FileSystem fs = HdfsOperationUtil.getFs();
        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));
        long startTimeSystemTime= System.currentTimeMillis();
        String text=null;
        while ((text=bufferedReader.readLine())!=null){

        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
