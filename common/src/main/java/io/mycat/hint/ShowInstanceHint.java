package io.mycat.hint;

public class ShowInstanceHint extends HintBuilder {
    @Override
    public String getCmd() {
        return "showInstances";
    }
}