package io.mycat.beans.mysql;

public enum PrepareStmtExecuteFlag {
    CURSOR_TYPE_NO_CURSOR((byte)0x00),
    CURSOR_TYPE_READ_ONLY((byte)0x01),
    CURSOR_TYPE_FOR_UPDATE((byte)0x02),
    CURSOR_TYPE_SCROLLABLE((byte)0x04);
    byte value;

    PrepareStmtExecuteFlag(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }
}
