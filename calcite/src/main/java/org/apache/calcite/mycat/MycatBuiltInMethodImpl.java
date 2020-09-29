package org.apache.calcite.mycat;

import org.apache.calcite.util.TimestampString;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;

public class MycatBuiltInMethodImpl {
    public static Temporal timestampStringToUnixTimestamp(String s) {
        if (s == null) {
            return null;
        }
        int i = s.lastIndexOf(".");
        if (i == -1) {
            LocalDateTime parse = LocalDateTime.parse(s);
            return parse;
        } else {
            long millisSinceEpoch = new TimestampString(s.substring(0, i)).getMillisSinceEpoch();
            int i1 = Integer.parseInt(s.substring(i + 1));
            Timestamp timestamp = new Timestamp(millisSinceEpoch);
            timestamp.setNanos(i1);
            return timestamp.toLocalDateTime();
        }
    }

        public static LocalDateTime timestampStringToUnixDate(String s) {
        if (s == null) {
            return null;
        }
        int i = s.lastIndexOf(".");
        if (i == -1) {
            LocalDateTime parse = LocalDateTime.parse(s);
            return parse;
        } else {
            long millisSinceEpoch = new TimestampString(s.substring(0, i)).getMillisSinceEpoch();
            int i1 = Integer.parseInt(s.substring(i + 1));
            Timestamp timestamp = new Timestamp(millisSinceEpoch);
            timestamp.setNanos(i1);
            return  timestamp.toLocalDateTime();
        }
    }
    public static void main(String[] args) {
        Timestamp timestamp = new Timestamp(0, 0, 0, 0, 0, 0, 0);
        int year = timestamp.getYear();
    }

    public static Duration timeStringToUnixDate(String s) {
        return timeStringToTimeDuration(s);
    }

    public static Duration timeStringToTimeDuration(String text) {
        if (text == null) {
            return null;
        }
        text = text.trim();
        if (text.isEmpty()) {
            return null;
        }
        int days = 0;
        int h = 0;
        int m = 0;
        int second = 0;
        int optional = text.lastIndexOf('.');
        int second_part = 0;
        if (optional != -1) {
            second_part = Integer.parseInt(text.substring(optional + 1));
            text = text.substring(0, optional);
        }
        switch (text.charAt(0)) {
            case '-': {//[-] DAYS [H]H:MM:SS
                int suff = text.indexOf(" ");
                days = Integer.parseInt(text.substring(0, suff));
                String[] split = text.split(":");
                h = Integer.parseInt(split[0]);
                m = Integer.parseInt(split[1]);
                second = Integer.parseInt(split[2]);
                break;
            }
            default: {
                String[] split = text.split(":");
                split[0] = split[0].trim();
                if (split.length == 3) {
                    if (split[0].contains(" ")) {
                        //DAYS [H]H:MM:SS
                        String[] s = split[0].split(" ");
                        days = Integer.parseInt(s[0]);
                        h = Integer.parseInt(s[0]);
                        m = Integer.parseInt(split[1]);
                        second = Integer.parseInt(split[2]);
                        break;
                    } else {
                        //[H]H:MM:SS
                        h = Integer.parseInt(split[0]);
                        m = Integer.parseInt(split[1]);
                        second = Integer.parseInt(split[2]);
                        break;
                    }
                } else if (split.length == 2) {
                    m = Integer.parseInt(split[0]);
                    second = Integer.parseInt(split[1]);
                    break;
                } else if (split.length == 1) {
                    String shor = split[0];
                    switch (shor.length()) {
                        case 1: {
                            //S
                            second = Integer.parseInt(shor);
                            break;
                        }
                        case 2: {
                            //[S]S
                            second = Integer.parseInt(shor);
                            break;
                        }
                        case 3: {
                            //MSS
                            m = Integer.parseInt(shor.substring(0, 1));
                            second = Integer.parseInt(shor.substring(1, 2));
                            break;
                        }
                        case 4: {
                            //[M]MSS
                            m = Integer.parseInt(shor.substring(0, 2));
                            second = Integer.parseInt(shor.substring(2, 4));
                            break;
                        }
                        case 5: {
                            //HMMSS
                            h = Integer.parseInt(shor.substring(0, 1));
                            m = Integer.parseInt(shor.substring(1, 3));
                            second = Integer.parseInt(shor.substring(3, 5));
                            break;
                        }
                        case 6: {
                            //[H]HMMSS
                            h = Integer.parseInt(shor.substring(0, 2));
                            m = Integer.parseInt(shor.substring(2, 4));
                            second = Integer.parseInt(shor.substring(4, 6));
                            break;
                        }
                    }

                    break;
                }
            }
        }
        return Duration.ofDays(days).plusHours(h).plusMinutes(m).plusSeconds(second).plusNanos(second_part);

    }

    public static LocalDate dateStringToUnixDate(String s) {
        int hyphen1 = s.indexOf(45);
        int y;
        int m;
        int d;
        if (hyphen1 < 0) {
            y = Integer.parseInt(s.trim());
            m = 1;
            d = 1;
        } else {
            y = Integer.parseInt(s.substring(0, hyphen1).trim());
            int hyphen2 = s.indexOf(45, hyphen1 + 1);
            if (hyphen2 < 0) {
                m = Integer.parseInt(s.substring(hyphen1 + 1).trim());
                d = 1;
            } else {
                m = Integer.parseInt(s.substring(hyphen1 + 1, hyphen2).trim());
                d = Integer.parseInt(s.substring(hyphen2 + 1).trim());
            }
        }

        return LocalDate.of(y, m, d);
    }

}
