package com.hdfsbuffer2.bufferinterface;

import java.nio.ByteBuffer;
import java.util.EventListener;

/**
 * HDFSBuffer DataOuput 输出接口
 * 编程人员自定义如何读取这样一个有序的顺序byteBuffer数据块
 */
public interface BufferdataOutputHandler extends EventListener {
    public void BufferdataOutput(ByteBuffer byteBuffer,int bufferindex);
}
