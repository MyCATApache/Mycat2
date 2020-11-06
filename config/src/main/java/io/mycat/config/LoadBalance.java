package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
public class LoadBalance {
    String defaultLoadBalance = "BalanceRandom";
    List<LoadBalanceConfig> loadBalances = new ArrayList<>();
}
