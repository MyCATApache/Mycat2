package cn.lightfish.wu.ast.query;

import cn.lightfish.wu.Op;
import cn.lightfish.wu.ast.base.NodeVisitor;
import cn.lightfish.wu.ast.base.Schema;
import lombok.Data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class ProjectSchema extends Schema {
    private final Schema schema;
    private final List<String> columnNames;
    private final List<FieldType> fieldSchemaList;

    public ProjectSchema(Schema schema, List<String> alias) {
        super(Op.PROJECT);
        this.schema = schema;
        this.columnNames = alias;

        List<FieldType> fields = schema.fields();

        this.fieldSchemaList = new ArrayList<>();
        for (FieldType field : fields) {
            String id = field.getId();
            String type = field.getType();
            fieldSchemaList.add(new FieldType(id, type));
        }

    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(fieldSchemaList);
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}