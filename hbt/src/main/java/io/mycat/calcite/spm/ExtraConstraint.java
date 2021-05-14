package io.mycat.calcite.spm;

import lombok.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ExtraConstraint {
    List<String> tables = new ArrayList<>();
}
