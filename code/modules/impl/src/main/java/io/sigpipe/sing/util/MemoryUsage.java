package io.sigpipe.sing.util;

public class MemoryUsage {

    private Runtime runtime = Runtime.getRuntime();

    public MemoryUsage() {

    }

    public long max() {
        return runtime.maxMemory();
    }

    public long total() {
        return runtime.totalMemory();
    }

    public long free() {
        return runtime.freeMemory();
    }

    public long used() {
        return total() - free();
    }
}
