package io.mycat.calcite.spm;

import lombok.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Constraint {
    String sql;
    List<SqlTypeName> paramTypes = new ArrayList<>();
}
