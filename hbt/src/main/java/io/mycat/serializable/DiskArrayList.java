/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package io.mycat.serializable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;

/**
 * ArrayList backed by disk
 *
 * @author enrico.olivelli
 */
public final class DiskArrayList<T> implements AutoCloseable, Iterable<T> {

    private ArrayList<T> buffer = new ArrayList<>();
    private boolean swapped = false;
    private final int swapThreshold;
    private boolean compressionEnabled = false;
    private final Serializer<T> serializer;
    private boolean closed;

    public interface Serializer<T> {

        T read(ExtendedDataInputStream oo) throws IOException;

        void write(T object, ExtendedDataOutputStream oo) throws IOException;
    }

    public void enableCompression() {
        if (swapped) {
            throw new RuntimeException("list already swapped, cannot enable compression now");
        }
        compressionEnabled = true;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    @Override
    // lettura
    public Iterator<T> iterator() {
        if (closed) {
            throw new IllegalArgumentException("this DiskArrayList has been closed");
        }
        if (!written) {
            throw new IllegalArgumentException("call finish() before read operations");
        }
        if (swapped) {
            return new Reader();
        } else {
            return buffer.iterator();
        }
    }

    // scrittura
    public void add(T summary) {
        try {
            size++;
            if (size > swapThreshold && !swapped) {
                startWrite();
            }
            if (!swapped) {
                buffer.add(summary);
            } else {
                serializer.write(summary, oout);
            }
        } catch (IOException err) {
            throw new RuntimeException(err);
        }
    }

    public boolean isSwapped() {
        return swapped;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public void sortBuffer(Comparator<T> comparator) {
        if (!written) {
            throw new IllegalArgumentException("call finish() before sort operations");
        }
        if (isSwapped()) {
            throw new IllegalStateException();
        }
        buffer.sort(comparator);
    }

    public void finish() {
        closeWriter();
    }

    public DiskArrayList(int swapThreshold, Path tmpDir, Serializer<T> serializer) {
        this.swapThreshold = swapThreshold;
        this.tmpDir = tmpDir;
        this.serializer = serializer;
    }

    @Override
    public void close() {
        closeWriter();
        closeReader();
        if (tmpFile != null) {
            logger.log(Level.FINER, "destroy tmp swap file {0}", tmpFile);
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException error) {
                logger.log(Level.SEVERE, "cannot delete tmp swap file {0}: " + error, tmpDir);
            }
            tmpFile = null;
        }
        written = false;
        swapped = false;
        closed = true;
    }

    private void startWrite() throws IOException {
        swapped = true;
        openWriter();
        for (T record : buffer) {
            serializer.write(record, oout);
        }
        buffer.clear();
        buffer = null;
    }

    public void truncate(int size) {
        if (size > 0 && this.size > size) {
            this.size = size;
            if (!swapped) {
                buffer = new ArrayList<>(buffer.subList(0, size));
            }
        }
    }

    private class Reader implements Iterator<T> {

        public Reader() {
            openReader();
        }

        @Override
        public boolean hasNext() {
            return countread < size;
        }

        @Override
        public T next() {
            try {
                countread++;
                T res = serializer.read(oin);
                if (countread == size) {
                    closeReader();
                }
                return res;
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported");
        }
    }

    private static final int DISK_BUFFER_SIZE = 1024 * 128;

    private void openReader() {
        if (!written) {
            throw new IllegalStateException("prima bisogna riempire la lista e chiamare finish()");
        }
        if (writing) {
            throw new IllegalStateException("scrittura ancora in corso");
        }
        if (!swapped) {
            throw new IllegalStateException("scrittura non avvenuta");
        }
        if (in != null) {
            // rewind support
            closeReader();
        }
        try {
            in = Files.newInputStream(tmpFile);
            bin = new BufferedInputStream(in, DISK_BUFFER_SIZE);
            if (compressionEnabled) {
                zippedin = new LZ4BlockInputStream(bin);
                oin = new ExtendedDataInputStream(zippedin);
            } else {
                oin = new ExtendedDataInputStream(bin);
            }
            countread = 0;
        } catch (IOException err) {
            closeReader();
            throw new RuntimeException(err);
        }

    }

    private void closeReader() {

        if (oin != null) {
            try {
                oin.close();
            } catch (IOException err) {
            } finally {
                oin = null;
            }
        }
        if (zippedin != null) {
            try {
                zippedin.close();
            } catch (IOException err) {
            } finally {
                zippedin = null;
            }
        }
        if (bin != null) {
            try {
                bin.close();
            } catch (IOException err) {
            } finally {
                bin = null;
            }
        }
        if (in != null) {
            try {
                in.close();
            } catch (IOException err) {
            } finally {
                in = null;
            }
        }
    }

    private void openWriter() throws IOException {
        if (written) {
            throw new IllegalStateException("list is already closed");
        }
        if (writing) {
            throw new IllegalStateException("already writing on this list");
        }
        if (compressionEnabled) {
            this.tmpFile = Files.createTempFile(tmpDir, "listswap", ".tmp.gz");
        } else {
            this.tmpFile = Files.createTempFile(tmpDir, "listswap", ".tmp");
        }
        logger.log(Level.FINE, "opening tmp swap file {0}", tmpFile.toAbsolutePath());
        writing = true;
        try {
            out = Files.newOutputStream(tmpFile);
            bout = new SimpleBufferedOutputStream(out, DISK_BUFFER_SIZE);
            if (compressionEnabled) {
                zippedout = new LZ4BlockOutputStream(out);
                oout = new ExtendedDataOutputStream(zippedout);
            } else {
                oout = new ExtendedDataOutputStream(bout);
            }

        } catch (IOException ex) {
            closeWriter();
            throw new RuntimeException(ex);
        }
    }

    private void closeWriter() {

        writing = false;
        written = true;

        if (oout != null) {
            try {
                oout.close();
            } catch (IOException ex) {
            } finally {
                oout = null;
            }
        }
        if (zippedout != null) {
            try {
                zippedout.close();
            } catch (IOException ex) {
            } finally {
                zippedout = null;
            }
        }
        if (bout != null) {
            try {
                bout.close();
            } catch (IOException ex) {
            } finally {
                bout = null;
            }
        }
        if (out != null) {
            try {
                out.close();
            } catch (IOException ex) {
            } finally {
                out = null;
            }
        }

    }

    private OutputStream out;
    private SimpleBufferedOutputStream bout;
    private OutputStream zippedout;
    private ExtendedDataOutputStream oout;
    private InputStream in;
    private BufferedInputStream bin;
    private ExtendedDataInputStream oin;
    private InputStream zippedin;
    private Path tmpFile;
    private boolean writing;
    private boolean written;
    private int size;
    private int countread;
    private static final Logger logger = Logger.getLogger(DiskArrayList.class.getName());
    private final Path tmpDir;
}
