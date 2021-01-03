package io.mycat.hint;

public class ShowNativeBackendHint extends HintBuilder {
    @Override
    public String getCmd() {
        return "showNativeBackends";
    }
}