/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import lombok.Getter;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduleUtil {
    final static ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

    public static ScheduledExecutorService getTimer() {
        return timer;
    }

    public static TimerTask getTimerFuture(Closeable closeable, long delay, TimeUnit unit) {
        TimerTask timerTask = new TimerTask(delay, unit) {

            @Override
            public void close() throws IOException {
                closeable.close();
            }
        };
        getTimer().schedule(() -> timerTask.accept(), delay, unit);
        return timerTask;
    }


    @Getter
    public static abstract class TimerTask {
        final long delay;
        final TimeUnit unit;
        final AtomicBoolean finished = new AtomicBoolean(false);

        TimerTask(long delay, TimeUnit unit) {
            this.delay = delay;
            this.unit = unit;
        }

        void accept() {
            if (!this.finished.get()) {
                onClose();
            }
        }

        public void setFinished() {
            finished.set(true);
        }

        abstract void close() throws IOException;

        private void onClose() {
            try {
                close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}