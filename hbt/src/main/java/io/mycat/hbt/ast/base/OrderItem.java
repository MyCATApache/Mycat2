package io.mycat.hbt.ast.base;

import io.mycat.hbt.ast.Direction;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderItem {
    Identifier columnName;
    Direction direction;
}