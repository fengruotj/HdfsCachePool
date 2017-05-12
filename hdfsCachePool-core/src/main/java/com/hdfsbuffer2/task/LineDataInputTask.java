package com.hdfsbuffer2.task;

import com.hdfsbuffer2.model.HDFSLineBuffer;
import com.hdfsbuffer2.util.HdfsOperationUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * locate com.hdfsbuffer2.task
 * Created by 79875 on 2017/4/21.
 * 将数据写入HDFSLineBuffer中
 */
public class LineDataInputTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LineDataInputTask.class);
    private long start;
    private long pos;
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private boolean isCompressedInput;
    private Decompressor decompressor;
    private byte[] recordDelimiterBytes;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private HDFSLineBuffer hdfsLineBuffer;

    private HdfsOperationUtil hdfsOperationUtil=new HdfsOperationUtil();

    private FileSplit fileSplit;

    private FileSystem fileSystem;//Hdfs文件系统

    private int block_num;

    public LineDataInputTask(HDFSLineBuffer hdfsLineBuffer, InputSplit inputSplit, int block_num) throws IOException {
        this.hdfsLineBuffer = hdfsLineBuffer;
        this.fileSplit= (FileSplit) inputSplit;
        this.block_num=block_num;
        fileSystem= HdfsOperationUtil.getFs();
    }

    public void initialize(InputSplit genericSplit) throws IOException {
        FileSplit split = (FileSplit)genericSplit;
        this.maxLineLength =  2147483647;
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(HdfsOperationUtil.getConf());
        this.fileIn = fs.open(file);
        CompressionCodec codec = (new CompressionCodecFactory(HdfsOperationUtil.getConf())).getCodec(file);
        if(null != codec) {
            this.isCompressedInput = true;
            this.decompressor = CodecPool.getDecompressor(codec);
            if(codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(this.fileIn, this.decompressor, this.start, this.end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new CompressedSplitLineReader(cIn, HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new SplitLineReader(codec.createInputStream(this.fileIn, this.decompressor), HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
                this.filePosition = this.fileIn;
            }
        } else {
            this.fileIn.seek(this.start);
            this.in = new SplitLineReader(this.fileIn, HdfsOperationUtil.getConf(), this.recordDelimiterBytes);
            this.filePosition = this.fileIn;
        }

        if(this.start != 0L) {
            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
        }

        this.pos = this.start;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput?2147483647:(int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if(this.isCompressedInput && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    private int skipUtfByteOrderMark() throws IOException {
        int newMaxLineLength = (int)Math.min(3L + (long)this.maxLineLength, 2147483647L);
        int newSize = this.in.readLine(this.value, newMaxLineLength, this.maxBytesToConsume(this.pos));
        this.pos += (long)newSize;
        int textLength = this.value.getLength();
        byte[] textBytes = this.value.getBytes();
        if(textLength >= 3 && textBytes[0] == -17 && textBytes[1] == -69 && textBytes[2] == -65) {
            LOG.info("Found UTF-8 BOM and skipped it");
            textLength -= 3;
            newSize -= 3;
            if(textLength > 0) {
                textBytes = this.value.copyBytes();
                this.value.set(textBytes, 3, textLength);
            } else {
                this.value.clear();
            }
        }

        return newSize;
    }

    public boolean nextKeyValue() throws IOException {
        if(this.key == null) {
            this.key = new LongWritable();
        }

        this.key.set(this.pos);
        this.value = new Text();//每一Value 都是新的实例对象

        int newSize = 0;

        while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
            if(this.pos == 0L) {
                newSize = this.skipUtfByteOrderMark();
            } else {
                newSize = this.in.readLine(this.value, this.maxLineLength, this.maxBytesToConsume(this.pos));
                this.pos += (long)newSize;
            }

            if(newSize == 0 || newSize < this.maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        if(newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void run() {

        try {
            initialize(this.fileSplit);
            //String str="";
            while (nextKeyValue()){
                //str=value.toString();
                hdfsLineBuffer.lineList.add(value);
            }
            LOG.debug("DataInputTask: size: "+hdfsLineBuffer.lineList.size()+" block_num: "+block_num);
            //System.gc();
            hdfsLineBuffer.setBufferFinished(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
