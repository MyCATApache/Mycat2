package io.mycat.exporter;

import com.google.common.collect.ImmutableList;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

import java.util.List;

public class CPULoadCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(CPULoadCollector.class);

    @Override
    public List<MetricFamilySamples> collect() {
        SystemInfo systemInfo = new SystemInfo();
        CentralProcessor processor = systemInfo.getHardware().getProcessor();
//        List<CentralProcessor.LogicalProcessor> logicalProcessors = processor.getLogicalProcessors();
        long[][] pTickList = processor.getProcessorCpuLoadTicks();

        GaugeMetricFamily gaugeMetricFamily = new GaugeMetricFamily("mycat_cpu_utility", "mycat_cpu_utility", ImmutableList.of("index"));

        long[][] tickList = processor.getProcessorCpuLoadTicks();
        for (int i = 0; i < pTickList.length; i++) {
            long[] prevTicks = pTickList[i];
            long[] ticks = tickList[i];

            long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
            long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
            long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
            long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()] - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
            long cSys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
            long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
            long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
            long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
            long totalCpu = user + nice + cSys + idle + iowait + irq + softirq + steal;

            double utility  = 1.0 - (idle * 1.0 / totalCpu);
            if (!Double.isInfinite(utility)) {
                utility = 0;
            }
            gaugeMetricFamily.addMetric(ImmutableList.of(String.valueOf(i)),utility);

        }




        return ImmutableList.of(gaugeMetricFamily);
    }
}