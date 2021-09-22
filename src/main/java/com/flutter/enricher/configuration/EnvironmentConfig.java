package com.flutter.enricher.configuration;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * Topology environment configurer that sets several important properties on the stream execution environment.
 * Some of these properties are just redefining their defaults but let's be aware of them.
 */
public class EnvironmentConfig {

    private static final String UI_REST_PORT_PROPERTY = "rest.port";
    private static final String GLOBAL_PARALLELISM = "execution.global.parallelism";


    /**
     * Configures the stream execution environment given a configurations on properties file.
     *
     * @throws IOException If state backend configuration breaks.
     */
    public static StreamExecutionEnvironment configureEnvironment(ParameterTool params) {
        // configure Web UI
        Configuration localUIConfiguration = new Configuration();
        localUIConfiguration.setString(UI_REST_PORT_PROPERTY, params.get(UI_REST_PORT_PROPERTY));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(localUIConfiguration);

        /*
         * Define global parallelism
         */
        env.setParallelism(params.getInt(GLOBAL_PARALLELISM));

        /*
         * Enable Checkpointing. Start a checkpoint every x ms
         */
        env.enableCheckpointing(params.getLong(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL.key()));

        /*
         * For local development, to make sure that the streaming application makes a certain amount of progress between
         * checkpoints, one can define how much time needs to pass between checkpoints.
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(params.getLong(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS.key()));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        ExecutionConfig executionConfig = env.getConfig();

        /*
         * RocksDB is an embedded database. If you are using RocksDB as your state backend for Flink, then each task manager
         * has a local instance of RocksDB, which runs as a native (JNI) library inside the JVM. When using RocksDB, your
         * state lives as serialized bytes on the local disk, with an in-memory (off-heap) cache.
         *
         * During checkpointing, the SST files from RocksDB are copied from the local disk to the distributed file system
         * where the checkpoint is stored. If the local recovery option is enabled, then a local copy is retained as well,
         * to speed up recovery. But it wouldn't be safe to rely only on the local copy, as the local disk might be lost
         * if the node fails. This is why checkpoints are always stored on a distributed file system.
         */
        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(
                params.getBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key())
        );
        backend.setDbStoragePath(params.getRequired(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key()));
        env.setStateBackend(backend);

        /*
         * Pass a mode to the enableCheckpointing(n) method to choose between the two guarantee levels.
         * Exactly-once is preferable for most applications.
         * At-least-once may be relevant for certain super-low-latency (consistently few milliseconds) applications.
         */
        checkpointConfig.setCheckpointingMode(
                CheckpointingMode.valueOf(params.getRequired(ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()))
        );

        /*
         * Make parameters available in the web interface
         */
        executionConfig.setGlobalJobParameters(params);

        return env;
    }
}
