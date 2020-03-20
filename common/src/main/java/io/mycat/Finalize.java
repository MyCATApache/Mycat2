package io.mycat;

import lombok.extern.log4j.Log4j;

@Log4j
public abstract class Finalize {

    protected abstract void close();

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } catch (Exception e) {
            log.error(e);
        } finally {
            super.finalize();
        }
    }
}