package minsait.ttaa.datio.common.naming;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.col;

public class Field {

    Field(String name) {
        this.name = name;
    }

    private String name = "field";

    public String getName() {
        return name;
    }

    public Column column() {
        return col(name);
    }
}
