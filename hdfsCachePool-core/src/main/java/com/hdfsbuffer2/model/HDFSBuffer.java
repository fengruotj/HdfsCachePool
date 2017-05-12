package com.hdfsbuffer2.model;

import java.nio.ByteBuffer;

/**
 * locate com.hdfsbuffer2.model
 * Created by 79875 on 2017/4/17.
 */
public class HDFSBuffer {
    public ByteBuffer byteBuffer;
    private boolean isBufferFinished=false;//是否缓存完毕，初始化缓存没完成
    private boolean isBufferOutFinished=true;//是否Buffer输出完毕，初始化没有输出完毕。

    private int inputSplitNum;  //当前HDFSBuffer缓冲的是哪一块inputSplit
    public HDFSBuffer(ByteBuffer byteBuffer,int inputSplitNum) {
        this.byteBuffer = byteBuffer;
        this.inputSplitNum=inputSplitNum;
    }

    public HDFSBuffer(int inputSplitNum) {
        this.inputSplitNum = inputSplitNum;
    }

    public boolean isBufferOutFinished() {
        return isBufferOutFinished;
    }

    public void setBufferOutFinished(boolean bufferOutFinished) {
        isBufferOutFinished = bufferOutFinished;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    public boolean isBufferFinished() {
        return isBufferFinished;
    }

    public void setBufferFinished(boolean bufferFinished) {
        isBufferFinished = bufferFinished;
    }

    public int getInputSplitNum() {

        return inputSplitNum;
    }

    public void setInputSplitNum(int inputSplitNum) {
        this.inputSplitNum = inputSplitNum;
    }
}
