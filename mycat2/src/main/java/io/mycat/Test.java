package io.mycat;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import org.apache.curator.shaded.com.google.common.collect.ImmutableSet;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Test {
    public static void main(String[] args) {
        //doStringNumber
        HashMap<String,Class> map = new HashMap<>();
        ImmutableSet<Class<?>> of = ImmutableSet.of(

                String.class,
                CharSequence.class,

                Byte.TYPE,
                Byte.class,

                Number.class,
                Short.TYPE,
                Short.class,
                Character.TYPE,
                Character.class,
                Integer.TYPE,
                Integer.class,
                Long.TYPE,
                Long.class,
                Float.TYPE,
                Float.class,
                Double.TYPE,
                Double.class,

                BigInteger.class,
                BigDecimal.class,

                Object.class,
                byte[].class,


                Boolean.TYPE,
                Boolean.class,

                UnsignedInteger.class,
                UnsignedLong.class,

                Date.class,
                Time.class,
//                java.sql.Date.class,
                Timestamp.class,
                LocalDate.class,
                LocalDateTime.class,
                LocalTime.class
        );
        ArrayList<String> list = new ArrayList<>();
        for (Class<?> in : of) {
            for (Class<?> out : of) {
//                if (in!=out){
//                    continue;
//                }


                String format = MessageFormat.format("public  {0} do{1}{2} ({3} in);",
                        fix(out)
                       , getFunName(in),
                        getFunName(out)
                        , fix(in));
                list.add(format);

            }
        }
        List<String> collect = list.stream().distinct().sorted().collect(Collectors.toList());
        System.out.println(collect.size());
        for (String s : collect) {
            System.out.println(s);
        }


    }

    @NotNull
    public static String getFunName(Class<?> in) {
        String inSimpleName = (in.getSimpleName());
        inSimpleName = (inSimpleName.replace("byte[]","PrimitiveByteArray"));
        if (in.isPrimitive()){
            inSimpleName = Character.toUpperCase(inSimpleName.charAt(0))+inSimpleName.substring(1,inSimpleName.length());
        }

        inSimpleName =  in.isPrimitive()?"Primitive"+inSimpleName:inSimpleName;
        return inSimpleName;
    }

    private static String fix(Class<?> in) {
        String name;
        if (in == byte[].class){
            name =  " byte[] ";
        }else {
            name = in.toString();
        }
        return name.replace("class "," ").replace("interface "," ");
    }
}