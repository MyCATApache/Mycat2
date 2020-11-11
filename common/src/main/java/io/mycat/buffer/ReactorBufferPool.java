package io.mycat.buffer;
/**
 * Reactor缓冲池
 *
 * @author chenjunwen
 * @version 1.0
 */
public interface ReactorBufferPool extends BufferPool {
    BufferPool newSessionBufferPool();
}
