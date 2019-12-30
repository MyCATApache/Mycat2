package cn.lightfish.wu.ast.base;

import cn.lightfish.wu.ast.Direction;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class OrderItem {
    Identifier columnName;
    Direction direction;
}