package io.mycat.swapbuffer;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PacketResponseImpl implements PacketResponse {
    final PacketRequest request;
    int copyCount;

    @Override
    public void setCopyCount(int n) {
        this.copyCount = n;
    }
}
