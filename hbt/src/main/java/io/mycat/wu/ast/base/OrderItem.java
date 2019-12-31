package io.mycat.wu.ast.base;

import io.mycat.wu.ast.Direction;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderItem {
    Identifier columnName;
    Direction direction;
}