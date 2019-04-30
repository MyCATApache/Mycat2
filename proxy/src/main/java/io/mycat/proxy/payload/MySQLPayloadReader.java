/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.payload;

public interface MySQLPayloadReader<T extends MySQLPayloadReader<T>> {
    int length();
    public long readFixInt(int length);
    public  int readLenencInt();
    public  String readFixString(int length);
    public  String readLenencString();
    public  byte[] readLenencStringBytes();
    public  byte[] readNULStringBytes();
    public  String readNULString();
    public  byte[] readEOFStringBytes();
    public  String readEOFString();
    public  byte[] readBytes(int length);
    public byte[] readFixStringBytes(int length);
    public  byte readByte();
    public  byte[] readLenencBytes();
    public long readLong();
    public double readDouble();
    public void reset();

    void skipInReading(int i);

    boolean readFinished();
}
