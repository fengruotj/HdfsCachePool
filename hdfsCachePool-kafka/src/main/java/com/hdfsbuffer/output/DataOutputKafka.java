package com.hdfsbuffer.output;

import com.hdfsbuffer.util.KafkaUtil;
import com.hdfsbuffer2.BufferLineReader;
import com.hdfsbuffer2.model.HdfsCachePool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * locate com.hdfsbuffer
 * Created by 79875 on 2017/4/11.
 * HDFSCachePoll 的数据输出类 数据输出到Kafka中
 */
public class DataOutputKafka {
    private static int threadNum=1;
    private static KafkaUtil kafkaUtil=new KafkaUtil(threadNum);

    private static final Log LOG = LogFactory.getLog(DataOutputKafka.class);
    private long Totalrows=0L;
    private long blockrows=0L;
    private HdfsCachePool hdfsCachePool;

    private int blockPosition=0;

    private  int kafkaParitionsNum;

    public DataOutputKafka(HdfsCachePool hdfsCachePool, int kafkaParitionsNum) {
        this.hdfsCachePool = hdfsCachePool;
        this.kafkaParitionsNum=kafkaParitionsNum;
    }

    public void datoutputKafka(final String kafkatopic) throws Exception {
        final String topic=kafkatopic;

        BufferLineReader bufferLineReader=new BufferLineReader(hdfsCachePool);
        Text text=new Text();
        try {
            while (bufferLineReader.readLine(text)!=0){
                Totalrows++;
                blockrows++;
                //kafkaUtil.publishMessage(kafkatopic, String.valueOf(Totalrows),text.toString());
                kafkaUtil.publishOrderMessage(topic,kafkaParitionsNum,(int)Totalrows,text.toString());
                //System.out.println(text.toString());
                //System.out.println(byteBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
