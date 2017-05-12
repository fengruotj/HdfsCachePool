package com.hdfsbuffer2.model;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.hdfsbuffer2.model
 * Created by 79875 on 2017/4/21.
 */
public class HDFSLineBuffer {
    public List<Text> lineList=new ArrayList<>();

    private boolean isBufferFinished=false;//是否缓存完毕，初始化缓存没完成
    private boolean isBufferOutFinished=true;//是否Buffer输出完毕，初始化没有输出完毕。

    private int inputSplitNum;  //当前HDFSBuffer缓冲的是哪一块inputSplit
    public HDFSLineBuffer(List<Text> lineList, int inputSplitNum) {
        this.lineList = lineList;
        this.inputSplitNum=inputSplitNum;
    }

    public HDFSLineBuffer(int inputSplitNum) {
        this.inputSplitNum = inputSplitNum;
    }

    public boolean isBufferOutFinished() {
        return isBufferOutFinished;
    }

    public void setBufferOutFinished(boolean bufferOutFinished) {
        isBufferOutFinished = bufferOutFinished;
    }

    public List<Text> getLineList() {
        return lineList;
    }

    public void setLineList(List<Text> lineList) {
        this.lineList = lineList;
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
