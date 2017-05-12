package com.hdfsbuffer2;

import com.hdfsbuffer2.model.HDFSBuffer;
import com.hdfsbuffer2.model.HdfsCachePool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 79875 on 2017/4/7.
 */
public class BufferLineReader {
    private static final Logger LOG = LoggerFactory.getLogger(BufferLineReader.class);
    private static final int DEFAULT_BUFFER_SIZE = 65536;
    private int bufferSize;
    private byte[] buffer;
    private int bufferLength;
    private int bufferPosn;
    private static final byte CR = 13;
    private static final byte LF = 10;
    private final byte[] recordDelimiterBytes;
    private HdfsCachePool hdfsCachePool;

    private int hdfsBufferIndex=0;    //HdfsCachePool ByteBuffer下标
    public BufferLineReader(HdfsCachePool hdfsCachePool) {
        this(hdfsCachePool , 65536);
    }

    public BufferLineReader(HdfsCachePool hdfsCachePool, int bufferSize) {
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = null;
        this.hdfsCachePool=hdfsCachePool;
    }

    public BufferLineReader(HdfsCachePool hdfsCachePool, Configuration conf) throws IOException {
        this(hdfsCachePool, conf.getInt("io.file.buffer.size", 65536));
    }

    public BufferLineReader(HdfsCachePool hdfsCachePool, byte[] recordDelimiterBytes) {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
        this.hdfsCachePool=hdfsCachePool;
    }

    public BufferLineReader(HdfsCachePool hdfsCachePool, int bufferSize, byte[] recordDelimiterBytes) {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.hdfsCachePool=hdfsCachePool;
        this.bufferSize = bufferSize;
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
    }

    public BufferLineReader(HdfsCachePool hdfsCachePool, Configuration conf, byte[] recordDelimiterBytes) throws IOException {
        this.bufferSize = 65536;
        this.bufferLength = 0;
        this.bufferPosn = 0;
        this.bufferSize = bufferSize;
        this.bufferSize = conf.getInt("io.file.buffer.size", 65536);
        this.buffer = new byte[this.bufferSize];
        this.recordDelimiterBytes = recordDelimiterBytes;
        this.hdfsCachePool=hdfsCachePool;
    }

    public int readLine(Text str, int maxLineLength, int maxBytesToConsume) throws Exception {
        return this.recordDelimiterBytes != null?this.readCustomLine(str, maxLineLength, maxBytesToConsume):this.readDefaultLine(str, maxLineLength, maxBytesToConsume);
    }

    /**
     * 等待CachePoolBuffer 缓存hdfsBuffer 完毕
     * @param hdfsBuffer
     */
    private void watiForCachePoolBuffer(HDFSBuffer hdfsBuffer){
        ByteBuffer byteBuffer=hdfsBuffer.byteBuffer;
        while (!hdfsBuffer.isBufferFinished()){
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        hdfsBuffer.setBufferOutFinished(false);
    }

    protected int fillBuffer( byte[] buffer, boolean inDelimiter)  {
        //byteBuffer.flip();
        if(hdfsBufferIndex >= hdfsCachePool.getInputSplitList().size())
            return -1;

        int index=hdfsBufferIndex% hdfsCachePool.getBufferNum();
        HDFSBuffer hdfsBuffer = hdfsCachePool.getBufferArray()[index];
        watiForCachePoolBuffer(hdfsBuffer);
        if(hdfsBuffer.byteBuffer.hasRemaining()){
            int bufferlength=Math.min(hdfsBuffer.byteBuffer.remaining(),buffer.length);
            hdfsBuffer.byteBuffer.get(buffer,0,bufferlength);
            if(!hdfsBuffer.byteBuffer.hasRemaining()){
                hdfsBuffer.setBufferOutFinished(true);
                hdfsBuffer.setBufferFinished(false);
                hdfsBufferIndex++;
            }
            return bufferlength;
        }else{
            if(hdfsBufferIndex < hdfsCachePool.getInputSplitList().size()){

                LOG.info("bufferLine output next bufferNum :"+hdfsBufferIndex);
                index=hdfsBufferIndex% hdfsCachePool.getBufferNum();
                HDFSBuffer bufferTmp = hdfsCachePool.getBufferArray()[index];
                watiForCachePoolBuffer(bufferTmp);
                int bufferlength=Math.min(hdfsBuffer.byteBuffer.remaining(),buffer.length);
                hdfsBuffer.byteBuffer.get(buffer,0,bufferlength);
                return bufferlength;
            }
            else {
                LOG.debug("retrun -1 :"+hdfsBufferIndex);
                return -1;
            }
        }
        //System.out.println("new :"+byteBuffer);
    }

    private int readDefaultLine(Text str, int maxLineLength, int maxBytesToConsume) throws Exception {
        str.clear();

        int txtLength = 0;
        int newlineLength = 0;
        boolean prevCharCR = false;
        long bytesConsumed = 0L;

        do {
            int startPosn = this.bufferPosn;
            if(this.bufferPosn >= this.bufferLength) {
                startPosn = this.bufferPosn = 0;
                if(prevCharCR) {
                    ++bytesConsumed;
                }

                this.bufferLength = this.fillBuffer( this.buffer, prevCharCR);
                if(this.bufferLength <= 0) {
                    break;
                }
            }

            while(this.bufferPosn < this.bufferLength) {
                if(this.buffer[this.bufferPosn] == 10) {
                    newlineLength = prevCharCR?2:1;
                    ++this.bufferPosn;
                    break;
                }

                if(prevCharCR) {
                    newlineLength = 1;
                    break;
                }

                prevCharCR = this.buffer[this.bufferPosn] == 13;
                ++this.bufferPosn;
            }

             int readLength = this.bufferPosn - startPosn;
            if(prevCharCR && newlineLength == 0) {
                --readLength;
            }

            bytesConsumed += (long)readLength;
            int appendLength = readLength - newlineLength;
            if(appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }

            if(appendLength > 0) {
                str.append(this.buffer, startPosn, appendLength);
                txtLength += appendLength;
            }
        } while(newlineLength == 0 && bytesConsumed < (long)maxBytesToConsume);

        if(bytesConsumed > 2147483647L) {
            throw new IOException("Too many bytes before newline: " + bytesConsumed);
        } else {
//            if(hdfsBufferIndex>26){
//                LOG.debug("retrun bytesConsumed :"+bytesConsumed+" Text:"+str.toString()+" bufferLength: "+this.bufferLength);
//            }
            return (int)bytesConsumed;
        }
    }

    private int readCustomLine(Text str, int maxLineLength, int maxBytesToConsume) throws Exception {
        str.clear();
        int txtLength = 0;
        long bytesConsumed = 0L;
        int delPosn = 0;
        int ambiguousByteCount = 0;

        do {
            int startPosn = this.bufferPosn;
            if(this.bufferPosn >= this.bufferLength) {
                startPosn = this.bufferPosn = 0;
                this.bufferLength = this.fillBuffer(this.buffer, ambiguousByteCount > 0);
                if(this.bufferLength <= 0) {
                    str.append(this.recordDelimiterBytes, 0, ambiguousByteCount);
                    break;
                }
            }

            for(; this.bufferPosn < this.bufferLength; ++this.bufferPosn) {
                if(this.buffer[this.bufferPosn] == this.recordDelimiterBytes[delPosn]) {
                    ++delPosn;
                    if(delPosn >= this.recordDelimiterBytes.length) {
                        ++this.bufferPosn;
                        break;
                    }
                } else if(delPosn != 0) {
                    --this.bufferPosn;
                    delPosn = 0;
                }
            }

            int readLength = this.bufferPosn - startPosn;
            bytesConsumed += (long)readLength;
            int appendLength = readLength - delPosn;
            if(appendLength > maxLineLength - txtLength) {
                appendLength = maxLineLength - txtLength;
            }

            if(appendLength > 0) {
                if(ambiguousByteCount > 0) {
                    str.append(this.recordDelimiterBytes, 0, ambiguousByteCount);
                    bytesConsumed += (long)ambiguousByteCount;
                    ambiguousByteCount = 0;
                }

                str.append(this.buffer, startPosn, appendLength);
                txtLength += appendLength;
            }

            if(this.bufferPosn >= this.bufferLength && delPosn > 0 && delPosn < this.recordDelimiterBytes.length) {
                ambiguousByteCount = delPosn;
                bytesConsumed -= (long)delPosn;
            }
        } while(delPosn < this.recordDelimiterBytes.length && bytesConsumed < (long)maxBytesToConsume );

        if(bytesConsumed > 2147483647L) {
            throw new IOException("Too many bytes before delimiter: " + bytesConsumed);
        } else {
            return (int)bytesConsumed;
        }
    }


    public int readLine(Text str, int maxLineLength) throws Exception {
        return this.readLine(str, maxLineLength, 2147483647);
    }

    public int readLine(Text str) throws Exception {
        return this.readLine(str, 2147483647, 2147483647);
    }
}
