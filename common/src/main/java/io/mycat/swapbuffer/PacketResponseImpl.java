package io.mycat.swapbuffer;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class PacketResponseImpl implements PacketResponse {
    final PacketRequest request;
    final int copyCount;
}
