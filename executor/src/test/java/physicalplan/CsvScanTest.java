package physicalplan;


import io.ordinate.engine.physicalplan.CsvScanPlan;
import io.ordinate.engine.physicalplan.PhysicalPlan;
import io.ordinate.engine.physicalplan.ValuesCsvScanPlan;
import io.ordinate.engine.schema.ArrowTypes;
import io.ordinate.engine.builder.ExecuteCompiler;
import io.ordinate.engine.record.RootContext;
import io.ordinate.engine.builder.SchemaBuilder;
import io.ordinate.engine.function.Function;
import io.reactivex.rxjava3.core.Observable;
import lombok.SneakyThrows;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class CsvScanTest  {

   // @Test
    @SneakyThrows
    public void baseTest(){

        Path path = Paths.get("D:\\testcsv.csv");
//
//        CsvWriter writer = new CsvWriter(path.toFile(), new CsvWriterSettings());
//        for (int i = 0; i < 800_0000; i++) {
//            writer.writeRow(Arrays.asList(i,i));
//        }
//        writer.close();
        StopWatch stopWatch = new StopWatch();



        ExecuteCompiler executeCompiler = new ExecuteCompiler();

        CsvScanPlan csvScan = new ValuesCsvScanPlan(path.toString(), SchemaBuilder.ofArrowType(ArrowTypes.INT64_TYPE, ArrowTypes.STRING_TYPE).toArrow(),executeCompiler.createRootContext());
        Function column = executeCompiler.column(0, csvScan.schema());
        Function add = executeCompiler.call("+",Arrays.asList( column, column));
        RootContext rootContext =  executeCompiler.createRootContext();
        PhysicalPlan projection =executeCompiler.project(csvScan,Arrays.asList(add));
       // Projection projection = new Projection(csvScan, , SchemaBuilder.of(ArrowTypes.Int64Type).toArrow());


        for (int i = 0; i < 100; i++) {
            stopWatch.reset();
            stopWatch.start();

           Observable<VectorSchemaRoot> execute = projection.execute(rootContext);
            AtomicLong count = new AtomicLong(0);
            execute.blockingLatest().forEach(c->{
                    count.getAndAdd(c.getRowCount());
                c.close();
            });

            stopWatch.stop();
            System.out.println("count:"+count);
            Duration duration = Duration.ofMillis(stopWatch.getTime());
            System.out.println(duration.getSeconds());
            System.out.println(stopWatch.toString());
        }

//
//        Thread.sleep(100000000);
    }
    public static String formatDateTime(long milliseconds) {
        StringBuilder sb = new StringBuilder();
        long mss = milliseconds / 1000;
        long days = mss / (60 * 60 * 24);
        long hours = (mss % (60 * 60 * 24)) / (60 * 60);
        long minutes = (mss % (60 * 60)) / 60;
        long seconds = mss % 60;
        DecimalFormat format = new DecimalFormat("00");
        if (days > 0 || hours > 0) {
            sb.append(format.format(hours)).append(":").append(format.format(minutes)).append(":").append(format.format(seconds));
        }else {
            sb.append(format.format(minutes)).append(":").append(format.format(seconds));
        }

        return sb.toString();
    }

}