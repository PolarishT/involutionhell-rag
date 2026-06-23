package com.involutionhell.backend.rag.infrastructure.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.stereotype.Component;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * GraalVM Native Image 运行时指标采集。
 *
 * <p>标准 Micrometer 的 {@code JvmMemoryMetrics}、{@code JvmGcMetrics} 等 Binder
 * 依赖 HotSpot 特有的 MXBean 扩展和 GC 通知机制，在 Native Image 下可能静默失效。
 * 本类直接通过 {@link ManagementFactory} 读取平台 MXBean，
 * 采集内存、CPU、线程、GC 四类指标，使用与标准 Binder 一致的命名约定，
 * 确保在 Kibana / Elasticsearch 中可直接沿用现有 Dashboard。
 *
 * <p>所有指标均基于 {@link MeterBinder} 接口注册，
 * Spring Boot Actuator 会在 {@link MeterRegistry} 就绪时自动调用 {@link #bindTo(MeterRegistry)}。
 */
@Component
public class NativeImageRuntimeMetrics implements MeterBinder {

    @Override
    public void bindTo(MeterRegistry registry) {
        bindMemoryMetrics(registry);
        bindCpuMetrics(registry);
        bindThreadMetrics(registry);
        bindGcMetrics(registry);
    }

    // ===== Memory =====

    private void bindMemoryMetrics(MeterRegistry registry) {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

        // Heap
        Gauge.builder("jvm.memory.used", memoryBean, b -> b.getHeapMemoryUsage().getUsed())
                .tag("area", "heap")
                .baseUnit("bytes")
                .description("Native Image heap memory used")
                .register(registry);

        Gauge.builder("jvm.memory.committed", memoryBean, b -> b.getHeapMemoryUsage().getCommitted())
                .tag("area", "heap")
                .baseUnit("bytes")
                .description("Native Image heap memory committed")
                .register(registry);

        Gauge.builder("jvm.memory.max", memoryBean, b -> b.getHeapMemoryUsage().getMax())
                .tag("area", "heap")
                .baseUnit("bytes")
                .description("Native Image heap memory max")
                .register(registry);

        // Non-heap
        Gauge.builder("jvm.memory.used", memoryBean, b -> b.getNonHeapMemoryUsage().getUsed())
                .tag("area", "nonheap")
                .baseUnit("bytes")
                .description("Native Image non-heap memory used")
                .register(registry);

        Gauge.builder("jvm.memory.committed", memoryBean, b -> b.getNonHeapMemoryUsage().getCommitted())
                .tag("area", "nonheap")
                .baseUnit("bytes")
                .description("Native Image non-heap memory committed")
                .register(registry);

        // Memory pools (heap regions in Native Image)
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            String poolName = normalizePoolName(pool.getName());
            boolean isHeap = pool.getType() == java.lang.management.MemoryType.HEAP;

            Gauge.builder("jvm.memory.used", pool, p -> p.getUsage().getUsed())
                    .tag("area", isHeap ? "heap" : "nonheap")
                    .tag("id", poolName)
                    .baseUnit("bytes")
                    .register(registry);

            Gauge.builder("jvm.memory.committed", pool, p -> p.getUsage().getCommitted())
                    .tag("area", isHeap ? "heap" : "nonheap")
                    .tag("id", poolName)
                    .baseUnit("bytes")
                    .register(registry);

            Gauge.builder("jvm.memory.max", pool, p -> {
                        long max = p.getUsage().getMax();
                        return max == -1 ? Double.NaN : max;
                    })
                    .tag("area", isHeap ? "heap" : "nonheap")
                    .tag("id", poolName)
                    .baseUnit("bytes")
                    .register(registry);
        }
    }

    // ===== CPU =====

    private void bindCpuMetrics(MeterRegistry registry) {
        var osBean = ManagementFactory.getOperatingSystemMXBean();

        Gauge.builder("system.cpu.count", osBean, b -> b.getAvailableProcessors())
                .description("Number of available processors")
                .register(registry);

        Gauge.builder("system.load.average.1m", osBean, b -> {
                    double load = b.getSystemLoadAverage();
                    return load < 0 ? Double.NaN : load;
                })
                .description("1-minute system load average")
                .register(registry);

        // com.sun.management.OperatingSystemMXBean provides process CPU usage
        if (osBean instanceof com.sun.management.OperatingSystemMXBean sunOsBean) {
            Gauge.builder("process.cpu.usage", sunOsBean, b -> b.getProcessCpuLoad())
                    .description("Process CPU usage (0.0 - 1.0)")
                    .register(registry);

            Gauge.builder("system.cpu.usage", sunOsBean, b -> b.getCpuLoad())
                    .description("System CPU usage (0.0 - 1.0)")
                    .register(registry);
        }
    }

    // ===== Threads =====

    private void bindThreadMetrics(MeterRegistry registry) {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        Gauge.builder("jvm.threads.live", threadBean, ThreadMXBean::getThreadCount)
                .description("Current live thread count")
                .register(registry);

        Gauge.builder("jvm.threads.daemon", threadBean, ThreadMXBean::getDaemonThreadCount)
                .description("Current daemon thread count")
                .register(registry);

        Gauge.builder("jvm.threads.peak", threadBean, ThreadMXBean::getPeakThreadCount)
                .description("Peak live thread count since start")
                .register(registry);
    }

    // ===== GC =====

    private void bindGcMetrics(MeterRegistry registry) {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();

        for (GarbageCollectorMXBean gc : gcBeans) {
            String gcName = normalizeGcName(gc.getName());

            Gauge.builder("jvm.gc.count", gc, GarbageCollectorMXBean::getCollectionCount)
                    .tag("action", gcName)
                    .description("GC collection count")
                    .register(registry);

            Gauge.builder("jvm.gc.time", gc, GarbageCollectorMXBean::getCollectionTime)
                    .tag("action", gcName)
                    .baseUnit("ms")
                    .description("Cumulative GC time")
                    .register(registry);
        }
    }

    // ===== Helpers =====

    /**
     * Normalize pool names like "Heap" → "heap", "Image heap" → "image-heap".
     */
    private String normalizePoolName(String name) {
        return name.toLowerCase().replace(' ', '-');
    }

    /**
     * Normalize GC names like "G1 Young Generation" → "g1-young".
     */
    private String normalizeGcName(String name) {
        return name.toLowerCase()
                .replace(" generation", "")
                .replace(' ', '-');
    }
}
