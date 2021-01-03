/**
 * Copyright (C) <2019>  <mycat>
 * <p>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.beans.mysql;

/**
 * A server version.
 */
public class ServerVersion implements Comparable<ServerVersion> {
    private String completeVersion;
    private Integer major;
    private Integer minor;
    private Integer subminor;

    public ServerVersion(String completeVersion, int major, int minor, int subminor) {
        this.completeVersion = completeVersion;
        this.major = major;
        this.minor = minor;
        this.subminor = subminor;
    }

    public ServerVersion(int major, int minor, int subminor) {
        this(null, major, minor, subminor);
    }

    /**
     * Parse the server version into major/minor/subminor.
     *
     * @param versionString
     *            string version representation
     * @return {@link ServerVersion}
     */
    public static ServerVersion parseVersion(final String versionString) {
        int point = versionString.indexOf('.');

        if (point != -1) {
            try {
                int serverMajorVersion = Integer.parseInt(versionString.substring(0, point));

                String remaining = versionString.substring(point + 1, versionString.length());
                point = remaining.indexOf('.');

                if (point != -1) {
                    int serverMinorVersion = Integer.parseInt(remaining.substring(0, point));

                    remaining = remaining.substring(point + 1, remaining.length());

                    int pos = 0;

                    while (pos < remaining.length()) {
                        if ((remaining.charAt(pos) < '0') || (remaining.charAt(pos) > '9')) {
                            break;
                        }

                        pos++;
                    }

                    int serverSubminorVersion = Integer.parseInt(remaining.substring(0, pos));

                    return new ServerVersion(versionString, serverMajorVersion, serverMinorVersion, serverSubminorVersion);
                }
            } catch (NumberFormatException NFE1) {
            }
        }

        // can't parse the server version
        return new ServerVersion(0, 0, 0);
    }

    public int getMajor() {
        return this.major;
    }

    public int getMinor() {
        return this.minor;
    }

    public int getSubminor() {
        return this.subminor;
    }

    /**
     * A string representation of this version. If this version was parsed from, or provided with, a "complete" string which may contain more than just the
     * version number, this string is returned verbatim. Otherwise, a string representation of the version numbers is given.
     *
     * @return string version representation
     */
    @Override
    public String toString() {
        if (this.completeVersion != null) {
            return this.completeVersion;
        }
        return String.format("%d.%d.%d", this.major, this.minor, this.subminor);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !ServerVersion.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        ServerVersion another = (ServerVersion) obj;
        if (this.getMajor() != another.getMajor() || this.getMinor() != another.getMinor() || this.getSubminor() != another.getSubminor()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 23;
        hash += 19 * hash + this.major;
        hash += 19 * hash + this.minor;
        hash += 19 * hash + this.subminor;
        return hash;
    }

    public int compareTo(ServerVersion other) {
        int c;
        if ((c = this.major.compareTo(other.getMajor())) != 0) {
            return c;
        } else if ((c = this.minor.compareTo(other.getMinor())) != 0) {
            return c;
        }
        return this.subminor.compareTo(other.getSubminor());
    }

    /**
     * Does this version meet the minimum specified by `min'?
     *
     * @param min
     *            The minimum version to compare against.
     * @return true if version meets the minimum specified by `min'
     */
    public boolean meetsMinimum(ServerVersion min) {
        return compareTo(min) >= 0;
    }
}
