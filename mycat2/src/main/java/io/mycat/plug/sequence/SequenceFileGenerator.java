/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.plug.sequence;

import io.mycat.config.SequenceConfig;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class SequenceFileGenerator implements SequenceHandler {
    protected static final Logger LOGGER = LoggerFactory
            .getLogger(SequenceFileGenerator.class);
    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer map;

    @Override
    public synchronized Number get() {
        long value = this.map.getLong(0);
        this.map.putLong(value++);
        return value;
    }

    @Override
    @SneakyThrows
    public void init(SequenceConfig args, long workerId) {
        Path fileName = Paths.get(Optional.ofNullable(args.getFileName()).orElseGet(() -> args.getName() + "SequenceFileGenerator.txt"));
        if (!Files.exists(fileName)) {
            Files.createFile(fileName);
        }
        this.randomAccessFile = new RandomAccessFile(fileName.toFile(), "rws");
        this.map = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 8);
    }

    @Override
    public synchronized void setStart(Number value) {
        if (this.map != null) {
            this.map.putLong(0, value.longValue());
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (this.randomAccessFile != null) {
            this.randomAccessFile.close();
        }
    }
}