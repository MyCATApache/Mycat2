package io.mycat;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;

public class MycatTimeUtil {

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
            String substring = text.substring(optional + 1);
            second_part = Integer.parseInt(
                    substring
            ) * (int) Math.pow(10, 9 - substring.length());
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
                if (text.contains(" ")) {
                    text = text.split(" ")[1];
                }
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

    public static Temporal timestampStringToTimestamp(String s) {
        if (s == null) {
            return null;
        }
        int i = s.lastIndexOf(".");
        if (i == -1) {
            i = s.lastIndexOf(" ");
            if (i != -1) {
                return Timestamp.valueOf(s).toLocalDateTime();
            }
            if (s.contains("-")) {
                //SELECT DATE_FORMAT('2006-06-00', '%d'); unsupport
                return LocalDate.parse(s).atStartOfDay();
            }
            if (s.contains(":")) {
                return LocalTime.parse(s);
            }
            if (Character.isDigit(s.charAt(s.length()-1))){//只有数字则默认是秒
                return LocalTime.of(0,0,Integer.parseInt(s));
            }
            throw new UnsupportedOperationException();
        } else {
            if (s.contains(" ")) {
                String[] uni = s.split(" ");
                return LocalDateTime.of(LocalDate.parse(uni[0]), LocalTime.parse(uni[1]));
            }
            return LocalTime.parse(s);
        }
    }
}
