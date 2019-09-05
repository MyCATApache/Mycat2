package io.mycat.calcite;

import io.mycat.router.RuleAlgorithm;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class DataMappingConfig {
    List<String> columnName;
    RuleAlgorithm ruleAlgorithm;
}