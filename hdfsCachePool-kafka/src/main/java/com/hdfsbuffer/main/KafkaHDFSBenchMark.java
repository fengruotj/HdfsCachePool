package com.hdfsbuffer.main;

import com.hdfsbuffer.util.KafkaUtil;
import com.hdfsbuffer2.util.HdfsOperationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;

/**
 * locate com.hdfsbuffer
 * Created by 79875 on 2017/4/19.
 * java -Xmx4028m -Xms4028m -cp hdfsCachePool-kafka-1.0-SNAPSHOT.jar com.hdfsbuffer.main.KafkaHDFSBenchMark /user/root/flinkwordcount/input/resultTweets.txt testTopic 9
 * 实验结果： testTopic	9	3	100	0	1	0		466885.85	4635049103
 */
public class KafkaHDFSBenchMark {
    private static final Log LOG = LogFactory.getLog(KafkaHDFSBenchMark.class);
    private static HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();
    private static KafkaUtil kafkaUtil=new KafkaUtil();
    private static byte[] recordDelimiterBytes;//记录分割符
    private static long Totalrows=0L;
    public static void main(String[] args) throws IOException {
        String inputFile=args[0];
        String topic=args[1];
        int partitionsNum=Integer.valueOf(args[2]);
        FileSystem fs = HdfsOperationUtil.getFs();
        FSDataInputStream dataInputStream = fs.open(new Path(inputFile));
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(dataInputStream));
        long startTimeSystemTime= System.currentTimeMillis();
        String text=null;
        while ((text=bufferedReader.readLine())!=null){
            Totalrows++;
            kafkaUtil.publishOrderMessage(topic,partitionsNum,(int)Totalrows,text);
        }
        long endTimeSystemTime = System.currentTimeMillis();
        LOG.info("startTime:"+new Timestamp(startTimeSystemTime));
        LOG.info("endTime:"+new Timestamp(endTimeSystemTime));
        long timelong = (endTimeSystemTime-startTimeSystemTime) / 1000;
        LOG.info("totalTime:"+timelong+" s"+"------or------"+timelong/60+" min");
        System.exit(0);
    }
}
