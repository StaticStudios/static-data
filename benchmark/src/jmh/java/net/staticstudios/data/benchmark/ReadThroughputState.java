package net.staticstudios.data.benchmark;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/** Shares one Static Data environment across all readers in a throughput trial. */
@State(Scope.Benchmark)
public class ReadThroughputState extends PlayerWorkloadState {
}
