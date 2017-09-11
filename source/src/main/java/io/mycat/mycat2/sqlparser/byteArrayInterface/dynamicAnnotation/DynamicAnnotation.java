package io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation;

import java.util.List;

/**
 * Created by jamie on 2017/9/5.
 */
public class DynamicAnnotation {
    List<String> match;
    List<String> actions;

    public static void main(String[] args) {

    }

    public List<String> getMatch() {
        return match;
    }

    public void setMatch(List<String> match) {
        this.match = match;
    }

    public List<String> getActions() {
        return actions;
    }

    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    @Override
    public String toString() {
        return "DynamicAnnotation{" +
                "match=" + match +
                ", actions=" + actions +
                '}';
    }
}
