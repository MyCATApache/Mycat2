package io.mycat.mycat2.sqlparser;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)//基准测试类型
@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
@Warmup(iterations = 3)//预热的迭代次数
@Threads(1)//测试线程数量
@State(Scope.Thread)//该状态为每个线程独享
//度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
@Measurement(iterations = 2, time = -1, timeUnit = TimeUnit.SECONDS, batchSize = -1)
//@CompilerControl() //http://javadox.com/org.openjdk.jmh/jmh-core/0.9/org/openjdk/jmh/annotations/CompilerControl.Mode.html
public class InstructionsBenchmark {
    static int staticPos = 0;
    //String src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));";
    final byte[] srcBytes = {83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 97, 98, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 44, 32, 101, 101, 46, 102, 102, 32, 65, 83, 32, 102, 44, 40, 83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 96, 115, 99, 104, 101, 109, 97, 95, 98, 98, 96, 46, 96, 116, 98, 108, 95, 98, 98, 96, 44, 40, 83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 99, 99, 99, 32, 65, 83, 32, 99, 44, 32, 96, 100, 100, 100, 100, 96, 41, 41, 59};
    int len = srcBytes.length;
    byte[] array = new byte[8192];
    int memberVariable = 0;

    //run
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(InstructionsBenchmark.class.getSimpleName())
                .forks(1)
                //     使用之前要安装hsdis
                //-XX:-TieredCompilation 关闭分层优化 -server
                //-XX:+LogCompilation  运行之后项目路径会出现按照测试顺序输出hotspot_pid<PID>.log文件,可以使用JITWatch进行分析,可以根据最后运行的结果的顺序按文件时间找到对应的hotspot_pid<PID>.log文件
                //.jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
                //  .addProfiler(CompilerProfiler.class)    // report JIT compiler profiling via standard MBeans
                //  .addProfiler(GCProfiler.class)    // report GC time
                // .addProfiler(StackProfiler.class) // report method stack execution profile
                // .addProfiler(PausesProfiler.class)
                /*
                WinPerfAsmProfiler
                You must install Windows Performance Toolkit. Once installed, locate directory with xperf.exe file
                and either add it to PATH environment variable, or set it to jmh.perfasm.xperf.dir system property.
                 */
                //.addProfiler(WinPerfAsmProfiler.class)
                //更多Profiler,请看JMH介绍
                //.output("InstructionsBenchmark.log")//输出信息到文件
                .build();
        new Runner(opt).run();
    }


    //空循环 对照项
    @Benchmark
    public int emptyLoop() {
        int pos = 0;
        while (pos < len) {
            ++pos;
        }
        return pos;
    }

    @Benchmark
    public int increment() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            ++result;
            ++pos;
        }
        return result;
    }

    @Benchmark
    public int decrement() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            --result;
            ++pos;
        }
        return result;
    }

    @Benchmark
    public int ifElse() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            if (pos == 10) {
                ++result;
                ++pos;
            } else {
                ++pos;
            }
        }
        return result;
    }

    @Benchmark
    public int ifElse2() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            if (pos == 10) {
                ++result;
                ++pos;
            } else if (pos == 20) {
                ++result;
                ++pos;
            } else {
                ++pos;
            }
        }
        return result;
    }

    @Benchmark
    public int ifnotElse() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            if (pos != 10) {
                ++pos;
            } else {
                ++result;
                ++pos;
            }
        }
        return result;
    }

    @Benchmark
    public int ifLessthanElse() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            if (pos < 10) {
                ++pos;
            } else {
                ++result;
                ++pos;
            }
        }
        return result;
    }

    @Benchmark
    public int ifGreaterthanElse() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            if (pos > 10) {
                ++pos;
            } else {
                ++result;
                ++pos;
            }
        }
        return result;
    }

    @Benchmark
    public int readMemberVariable_a_byteArray() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = srcBytes[pos];
            pos++;
        }
        return result;
    }

    @Benchmark
    public int writeMemberVariable_a_byteArray() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            array[pos] = 0;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int leftShift() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result << pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int leftShift8() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result << 8;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int rightShift() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result >> pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int rightShift8() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result >> 8;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int or() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result | pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int or8() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result | 8;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int and() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result & pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int and8() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = result & 8;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int add1000000() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result += 1000000;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int add() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result += pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int minus1000000() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result -= 1000000;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int minus() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result -= pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int multiple10() {
        int pos = 0;
        int result = 1;
        while (pos < len) {
            result *= 10;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int multiple() {
        int pos = 0;
        int result = 1;
        while (pos < len) {
            result *= pos;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int divide2() {
        int pos = 0;
        int result = 0x7fffffff;
        while (pos < len) {
            result /= 2;
            pos++;
        }
        return result;
    }

    @Benchmark
    public int divide() {
        int pos = 1;
        int result = 0x7fffffff;
        int l = len + 1;
        while (pos < l) {
            result /= pos;
            pos++;
        }
        return result;
    }

    @Benchmark//与数值有关,会恶化
    public int mod2() {
        int pos = 0;
        int result = 0;
        while (pos < len) {
            result = pos % 2;
            pos++;
        }
        return result;
    }

    @Benchmark//与数值有关,会恶化
    public int mod() {
        int pos = 1;
        int l = len + 1;
        int result = 0x7fffffff;
        while (pos < l) {
            result = result % pos;
            pos++;
        }
        return result;
    }

    //static 只读会inline的
    @Benchmark
    public int staticReadWrite() {
        staticPos = 0;
        int result = 0;
        while (InstructionsBenchmark.staticPos < len) {
            staticPos++;
        }
        return result;
    }

    @Benchmark
    public int staticWrite_a_int() {
        staticPos = 0;
        int result = 0;
        int pos = 0;
        while (pos < len) {
            ++staticPos;
            ++pos;
        }
        return result;
    }

    @Benchmark
    public byte[] newByteArrayVarLen() {
        int pos = 0;
        byte[] array = null;
        while (pos < len) {
            array = new byte[pos + 1];
            pos++;
        }
        return array;
    }

    @Benchmark
    public byte[] newByteArrayFinalLen() {
        int pos = 0;
        byte[] array = null;
        while (pos < len) {
            array = new byte[len];
            pos++;
        }
        return array;
    }

    @Benchmark
    public int writeMemberVariable_a_int() {
        int pos = 0;
        memberVariable = 0;
        while (pos < len) {
            ++memberVariable;
            pos++;
        }
        return memberVariable;
    }

    @Benchmark
    public int[] initIntArrayByNew() {
        int pos = 0;
        int[] array = null;
        while (pos < len) {
            array = new int[16];
            pos++;
        }
        return array;
    }

    int[] initIntArrayArraysfillArray = new int[16];

    @Benchmark
    public int[] initIntArrayArraysfill() {
        int pos = 0;
        while (pos < len) {
            Arrays.fill(initIntArrayArraysfillArray, 0);
            pos++;
        }
        return initIntArrayArraysfillArray;
    }

    @Benchmark
    public int switchJump256() {
        int pos = 0;
        while (pos < len) {
            switch (pos) {
                case 0:
                    ++pos;
                    break;
                case 1:

                case 2:

                case 3:

                case 4:

                case 5:

                case 6:

                case 7:

                case 8:

                case 9:

                case 10:

                case 11:

                case 12:

                case 13:

                case 14:

                case 15:

                case 16:

                case 17:

                case 18:

                case 19:

                case 20:

                case 21:
                    ++pos;
                    break;
                case 22:

                case 23:

                case 24:

                case 25:

                case 26:

                case 27:

                case 28:

                case 29:

                case 30:

                case 31:

                case 32:

                case 33:

                case 34:
                    ++pos;
                    break;
                case 35:

                case 36:

                case 37:

                case 38:

                case 39:

                case 40:

                case 41:

                case 42:

                case 43:

                case 44:

                case 45:
                    ++pos;
                    break;
                case 46:

                case 47:

                case 48:

                case 49:

                case 50:

                case 51:

                case 52:

                case 53:

                case 54:

                case 55:

                case 56:

                case 57:

                case 58:

                case 59:

                case 60:

                case 61:

                case 62:

                case 63:
                    ++pos;
                    break;
                case 64:

                case 65:

                case 66:

                case 67:

                case 68:

                case 69:

                case 70:

                case 71:

                case 72:

                case 73:

                case 74:

                case 75:

                case 76:

                case 77:

                case 78:

                case 79:

                case 80:

                case 81:

                case 82:

                case 83:

                case 84:

                case 85:

                case 86:

                case 87:

                case 88:

                case 89:

                case 90:

                case 91:

                case 92:

                case 93:

                case 94:

                case 95:

                case 96:

                case 97:

                case 98:

                case 99:

                case 100:

                case 101:

                case 102:

                case 103:

                case 104:

                case 105:

                case 106:

                case 107:

                case 108:

                case 109:

                case 110:

                case 111:

                case 112:

                case 113:

                case 114:

                case 115:

                case 116:

                case 117:

                case 118:

                case 119:

                case 120:

                case 121:

                case 122:

                case 123:

                case 124:

                case 125:

                case 126:

                case 127:

                case 128:

                case 129:

                case 130:

                case 131:

                case 132:

                case 133:

                case 134:

                case 135:

                case 136:

                case 137:

                case 138:

                case 139:

                case 140:

                case 141:

                case 142:

                case 143:

                case 144:

                case 145:

                case 146:

                case 147:

                case 148:

                case 149:

                case 150:

                case 151:

                case 152:

                case 153:

                case 154:

                case 155:

                case 156:

                case 157:

                case 158:

                case 159:

                case 160:

                case 161:

                case 162:

                case 163:

                case 164:

                case 165:

                case 166:

                case 167:

                case 168:

                case 169:

                case 170:

                case 171:

                case 172:

                case 173:

                case 174:

                case 175:

                case 176:

                case 177:
                    ++pos;
                    break;
                case 178:

                case 179:

                case 180:

                case 181:

                case 182:

                case 183:

                case 184:

                case 185:

                case 186:

                case 187:

                case 188:

                case 189:

                case 190:

                case 191:

                case 192:

                case 193:

                case 194:

                case 195:

                case 196:

                case 197:

                case 198:

                case 199:

                case 200:

                case 201:

                case 202:

                case 203:

                case 204:

                case 205:

                case 206:

                case 207:

                case 208:

                case 209:

                case 210:

                case 211:

                case 212:

                case 213:

                case 214:

                case 215:

                case 216:

                case 217:

                case 218:

                case 219:

                case 220:

                case 221:

                case 222:

                case 223:

                case 224:

                case 225:

                case 226:

                case 227:

                case 228:

                case 229:

                case 230:

                case 231:

                case 232:

                case 233:
                    ++pos;
                    break;
                case 234:

                case 235:

                case 236:

                case 237:

                case 238:

                case 239:

                case 240:

                case 241:

                case 242:

                case 243:

                case 244:

                case 245:

                case 246:

                case 247:

                case 248:

                case 249:

                case 250:

                case 251:

                case 252:

                case 253:

                case 254:

                case 255:

                default:
                    ++pos;
                    break;

            }
        }
        return pos;
    }

    @Benchmark
    public int switchJump64() {
        int pos = 0;
        while (pos < len) {
            switch (pos) {
                case 0:
                    ++pos;
                    break;
                case 1:

                case 2:

                case 3:

                case 4:

                case 5:

                case 6:

                case 7:

                case 8:

                case 9:

                case 10:

                case 11:

                case 12:

                case 13:

                case 14:

                case 15:

                case 16:

                case 17:

                case 18:

                case 19:

                case 20:

                case 21:
                    ++pos;
                    break;
                case 22:

                case 23:

                case 24:

                case 25:

                case 26:

                case 27:

                case 28:

                case 29:

                case 30:

                case 31:

                case 32:

                case 33:

                case 34:
                    ++pos;
                    break;
                case 35:

                case 36:

                case 37:

                case 38:

                case 39:

                case 40:

                case 41:

                case 42:

                case 43:

                case 44:

                case 45:
                    ++pos;
                    break;
                case 46:

                case 47:

                case 48:

                case 49:

                case 50:

                case 51:

                case 52:

                case 53:

                case 54:

                case 55:

                case 56:

                case 57:

                case 58:

                case 59:

                case 60:

                case 61:

                case 62:

                case 63:
                    ++pos;
                    break;
                case 64:
                default:
                    ++pos;
                    break;

            }
        }
        return pos;
    }
}