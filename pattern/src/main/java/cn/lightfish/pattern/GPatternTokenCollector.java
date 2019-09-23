package cn.lightfish.pattern;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public interface GPatternTokenCollector {
    GPatternTokenCollector EMPTY = new GPatternTokenCollector() {

        @Override
        public void onCollectStart() {

        }

        @Override
        public void collect(GPatternSeq token) {

        }

        @Override
        public void onCollectEnd() {

        }
    };

    void onCollectStart();

    void collect(GPatternSeq token);

    void onCollectEnd();
}