package io.mycat.beans.log.monitor;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatServer;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;


@Data

public class InstanceEntry implements LogEntry {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceEntry.class);
    double cpu;
    double mem;

    double lqps;
    double pqps;

    double lrt;
    double prt;

    double thread;

    double con;


    public InstanceEntry snapshot() {
        SystemInfo systemInfo = new SystemInfo();

        CentralProcessor processor = systemInfo.getHardware().getProcessor();
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        long[] ticks = processor.getSystemCpuLoadTicks();
        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()] - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
        long cSys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long totalCpu = user + nice + cSys + idle + iowait + irq + softirq + steal;

        double cpuRes = 1.0 - (idle * 1.0 / totalCpu);
        if (Double.isNaN(cpuRes)) {
            cpuRes = 0;
        }
        if (Double.isInfinite(cpuRes)) {
            cpuRes = 1;
        }
        GlobalMemory memory = systemInfo.getHardware().getMemory();
        long totalByte = memory.getTotal();
        long acaliableByte = memory.getAvailable();

        double memRes = (totalByte - acaliableByte) * 1.0 / totalByte;
        if (Double.isNaN(memRes)) {
            cpuRes = 0;
        }
        if (Double.isInfinite(memRes)) {
            cpuRes = 1;
        }

        InstanceEntry e = new InstanceEntry();
        e.cpu = cpuRes;
        e.mem = memRes;
        e.lqps = InstanceMonitor.getLqps();
        e.pqps = InstanceMonitor.getPqps();
        e.lrt = InstanceMonitor.getLrt();
        e.prt = InstanceMonitor.getPrt();
        e.thread = MetaClusterCurrent.wrapper(IOExecutor.class).count();
        e.con = MetaClusterCurrent.wrapper(MycatServer.class).countConnection();

        return e;
    }


}
