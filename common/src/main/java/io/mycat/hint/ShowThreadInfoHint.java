package io.mycat.hint;

public class ShowThreadInfoHint extends HintBuilder {
    public static String create() {
        ShowThreadInfoHint showThreadInfoHint = new ShowThreadInfoHint();
        return showThreadInfoHint.build();
    }

    @Override
    public String getCmd() {
        return "showThreadInfo";
    }
}