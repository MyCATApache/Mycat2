package io.mycat.proxy.session;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MycatUser {
  String userName;
  String password;
  String host;
}
