/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.schemabuilder;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

/**
 * The table options used in a CREATE TABLE or ALTER TABLE statement.
 * <p>
 * Implementation notes: this class is abstract and not meant to use directly.
 * The type parameter {@code T} allows usage of <strong>covariant return type</strong> and makes the builder pattern work for different sub-classes.
 *
 * @param <T> the concrete sub-class of {@link com.datastax.driver.core.schemabuilder.TableOptions}
 *
 * @see <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html" target="_blank">details on table options</a>
 */
public abstract class TableOptions<T extends TableOptions> {

    private SchemaStatement schemaStatement;

    private Optional<SchemaBuilder.Caching> caching = Optional.absent();
    private Optional<CachingRowsPerPartition> cachingRowsPerPartition = Optional.absent();

    private Optional<Double> bloomFilterFPChance = Optional.absent();

    private Optional<String> comment = Optional.absent();

    private Optional<CompressionOptions> compressionOptions = Optional.absent();

    private Optional<CompactionOptions> compactionOptions = Optional.absent();

    private Optional<Double> dcLocalReadRepairChance = Optional.absent();

    private Optional<Integer> defaultTTL = Optional.absent();

    private Optional<Long> gcGraceSeconds = Optional.absent();

    private Optional<Integer> indexInterval = Optional.absent();
    private Optional<Integer> minIndexInterval = Optional.absent();
    private Optional<Integer> maxIndexInterval = Optional.absent();

    private Optional<Long> memtableFlushPeriodInMillis = Optional.absent();

    private Optional<Boolean> populateIOOnCacheFlush = Optional.absent();

    private Optional<Double> readRepairChance = Optional.absent();

    private Optional<Boolean> replicateOnWrite = Optional.absent();

    private Optional<SpeculativeRetryValue> speculativeRetry = Optional.absent();

    private List<RawOption> customOptions = new ArrayList<RawOption>();

    protected T self;

    @SuppressWarnings("unchecked") TableOptions(SchemaStatement schemaStatement) {
        this.schemaStatement = schemaStatement;
        this.self = (T)this;
    }

    /**
     * Define the caching type for Cassandra 2.0.x.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is {@link SchemaBuilder.Caching#KEYS_ONLY}.
     *
     * @param caching the caching type (all enum values are allowed).
     * @return this {@code TableOptions} object.
     */
    public T caching(SchemaBuilder.Caching caching) {
        this.caching = Optional.fromNullable(caching);
        return self;
    }

    /**
     * Define the caching options for Cassandra 2.1.x.
     * <p>
     * If no call is made to this method, the default values set by Cassandra are keys = {@link SchemaBuilder.Caching#ALL} and
     * rows_per_partition = {@link com.datastax.driver.core.schemabuilder.SchemaBuilder#noRows()}.

     * @param keys the key cache type
     *             (allowed values: {@link SchemaBuilder.Caching#NONE} or {@link SchemaBuilder.Caching#ALL}).
     * @param rowsPerPartition defines the number of rows to be cached per partition when Row Caching is enabled.
     *                         To create instances, use
     *                         {@link SchemaBuilder#noRows()},
     *                         {@link SchemaBuilder#allRows()} or
     *                         {@link SchemaBuilder#rows(int)}.
     * @return this {@code TableOptions} object.
     */
    public T caching(SchemaBuilder.Caching keys, CachingRowsPerPartition rowsPerPartition) {
        this.caching = Optional.fromNullable(keys);
        this.cachingRowsPerPartition = Optional.fromNullable(rowsPerPartition);
        return self;
    }

    /**
     * Define the desired false-positive probability for SSTable Bloom filters.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is:
     * <ul>
     *     <li><strong>0.01</strong> for the size-tiered compaction strategy;</li>
     *     <li><strong>0.1</strong> for the leveled compaction strategy.</li>
     * </ul>
     *
     * @param fpChance the false positive change. This value should be between 0 and 1.0.
     * @return this {@code TableOptions} object.
     */
    public T bloomFilterFPChance(Double fpChance) {
        validateRateValue(fpChance, "Bloom filter false positive change");
        this.bloomFilterFPChance = Optional.fromNullable(fpChance);
        return self;
    }

    /**
     * Define a human readable comment describing the table.
     *
     * @param comment the comment.
     * @return this {@code TableOptions} object.
     */
    public T comment(String comment) {
        this.comment = Optional.fromNullable(comment);
        return self;
    }

    /**
     * Define the compression options.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is {@link SchemaBuilder#lz4()}.
     *
     * @param compressionOptions the compression options. To create instances, use
     *                           {@link SchemaBuilder#noCompression()},
     *                           {@link SchemaBuilder#lz4()},
     *                           {@link SchemaBuilder#snappy()} or
     *                           {@link SchemaBuilder#deflate()}.
     * @return this {@code TableOptions} object.
     */
    public T compressionOptions(CompressionOptions compressionOptions) {
        this.compressionOptions = Optional.fromNullable(compressionOptions);
        return self;
    }

    /**
     * Define the compaction options.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is {@link SchemaBuilder#sizedTieredStategy()}.
     *
     * @param compactionOptions the compaction options. To create instances, use
     *                          {@link SchemaBuilder#sizedTieredStategy()},
     *                          {@link SchemaBuilder#leveledStrategy()} or
     *                          {@link SchemaBuilder#dateTieredStrategy()}
     * @return this {@code TableOptions} object.
     */
    public T compactionOptions(CompactionOptions compactionOptions) {
        this.compactionOptions = Optional.fromNullable(compactionOptions);
        return self;
    }

    /**
     * Define the probability of read repairs being invoked over all replicas in the current data center.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 0.0.
     *
     * @param dcLocalReadRepairChance the probability.
     * @return this {@code TableOptions} object.
     */
    public T dcLocalReadRepairChance(Double dcLocalReadRepairChance) {
        validateRateValue(dcLocalReadRepairChance, "DC local read repair chance");
        this.dcLocalReadRepairChance = Optional.fromNullable(dcLocalReadRepairChance);
        return self;
    }

    /**
     * Define the default expiration time in seconds for a table.
     *
     * <p>
     * Used in MapReduce/Hive scenarios when you have no control of TTL.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 0.
     *
     * @param defaultTimeToLive the default time to live in seconds for a table.
     * @return this {@code TableOptions} object.
     */
    public T defaultTimeToLive(Integer defaultTimeToLive) {
        this.defaultTTL = Optional.fromNullable(defaultTimeToLive);
        return self;
    }

    /**
     * Define the time to wait before garbage collecting tombstones (deletion markers).
     *
     * The default value allows a great deal of time for consistency to be achieved prior to deletion.
     * In many deployments this interval can be reduced, and in a single-node cluster it can be safely set to zero.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 864000 secs (10 days).
     *
     * @param gcGraceSeconds the grace period.
     * @return this {@code TableOptions} object.
     */
    public T gcGraceSeconds(Long gcGraceSeconds) {
        this.gcGraceSeconds = Optional.fromNullable(gcGraceSeconds);
        return self;
    }

    /**
     * Define the index interval for Cassandra 2.0.
     *
     * To control the sampling of entries from the primary row index, configure sample frequency of the partition summary by changing the index interval.
     * After changing the index interval, SSTables are written to disk with new information. The interval corresponds to the number of index entries that
     * are skipped between taking each sample. By default, Cassandra samples one row key out of every 128. The larger the interval, the smaller and less
     * effective the sampling. The larger the sampling, the more effective the index, but with increased memory usage. In Cassandra 2.0.x, generally, the
     * best trade off between memory usage and performance is a value between 128 and 512 in combination with a large table key cache. However, if you have
     * small rows (many to an OS page), you may want to increase the sample size, which often lowers memory usage without an impact on performance. For
     * large rows, decreasing the sample size may improve read performance.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 128.
     *
     * @param indexInterval the index interval.
     * @return this {@code TableOptions} object.
     */
    public T indexInterval(Integer indexInterval) {
        this.indexInterval = Optional.fromNullable(indexInterval);
        return self;
    }

    /**
     * Define the minimum index interval for Cassandra 2.1.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 128.
     *
     * @param minIndexInterval the minimum index interval.
     * @return this {@code TableOptions} object.
     *
     * @see #indexInterval(Integer)
     */
    public T minIndexInterval(Integer minIndexInterval) {
        this.minIndexInterval = Optional.fromNullable(minIndexInterval);
        return self;
    }

    /**
     * Define the maximum index interval for Cassandra 2.1.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 2048.
     *
     * @param maxIndexInterval the maximum index interval.
     * @return this {@code TableOptions} object.
     *
     * @see #indexInterval(Integer)
     */
    public T maxIndexInterval(Integer maxIndexInterval) {
        this.maxIndexInterval = Optional.fromNullable(maxIndexInterval);
        return self;
    }

    /**
     * Define the memtable flush period.
     *
     * If set, this forces flushing of the memtable after the specified time elapses.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 0.
     *
     * @param memtableFlushPeriodInMillis the memtable flush period in milliseconds.
     * @return this {@code TableOptions} object.
     */
    public T memtableFlushPeriodInMillis(Long memtableFlushPeriodInMillis) {
        this.memtableFlushPeriodInMillis = Optional.fromNullable(memtableFlushPeriodInMillis);
        return self;
    }

    /**
     * Define whether to populate IO cache on flush of sstables.
     *
     * If set, Cassandra adds newly flushed or compacted sstables to the operating system page cache, potentially evicting other cached data to make room.
     * Enable when all data in the table is expected to fit in memory.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is {@code false}.
     *
     * @param populateIOOnCacheFlush whether to populate IO cache on flush of sstables.
     * @return this {@code TableOptions} object.
     *
     * @see <a href="http://www.datastax.com/documentation/cassandra/2.0/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__compaction_preheat_key_cache">the global option compaction_preheat_key_cache</a>
     */
    public T populateIOCacheOnFlush(Boolean populateIOOnCacheFlush) {
        this.populateIOOnCacheFlush = Optional.fromNullable(populateIOOnCacheFlush);
        return self;
    }

    /**
     * Define the probability with which read repairs should be invoked on non-quorum reads. The value must be between 0 and 1.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is 0.1.
     *
     * @param readRepairChance the read repair chance.
     * @return this {@code TableOptions} object.
     */
    public T readRepairChance(Double readRepairChance) {
        validateRateValue(readRepairChance, "Read repair chance");
        this.readRepairChance = Optional.fromNullable(readRepairChance);
        return self;
    }

    /**
     * Define whether to replicate data on write (Cassandra 2.0.x only).
     *
     * When set to {@code true}, replicates writes to all affected replicas regardless of the consistency level specified by the client for a write request.
     * For counter tables, this should always be set to {@code true}.
     * <p>
     * If no call is made to this method, the default value set by Cassandra is {@code true}.
     *
     * @param replicateOnWrite whether to replicate data on write.
     * @return this {@code TableOptions} object.
     */
    public T replicateOnWrite(Boolean replicateOnWrite) {
        this.replicateOnWrite = Optional.fromNullable(replicateOnWrite);
        return self;
    }

    /**
     * To override normal read timeout when read_repair_chance is not 1.0, sending another request to read, choose one of these values and use the property to create
     * or alter the table:
     * <ul>
     *     <li>ALWAYS: Retry reads of all replicas.</li>
     *     <li>Xpercentile: Retry reads based on the effect on throughput and latency.</li>
     *     <li>Yms: Retry reads after specified milliseconds.</li>
     *     <li>NONE: Do not retry reads.</li>
     * </ul>
     *
     * Using the speculative retry property, you can configure rapid read protection in Cassandra 2.0.2 and later.
     * Use this property to retry a request after some milliseconds have passed or after a percentile of the typical read latency has been reached,
     * which is tracked per table.
     *
     * <p>
     *     If not set, default = 99percentile Cassandra 2.0.2 and later
     * </p>
     * @param speculativeRetry the speculative retry. To create instances, use
     *                         {@link SchemaBuilder#noSpeculativeRetry()},
     *                         {@link SchemaBuilder#always()},
     *                         {@link SchemaBuilder#percentile(int)} or
     *                         {@link SchemaBuilder#millisecs(int)}.
     * @return this {@code TableOptions} object.
     */
    public T speculativeRetry(SpeculativeRetryValue speculativeRetry) {
        this.speculativeRetry = Optional.fromNullable(speculativeRetry);
        return self;
    }

    /**
     * Define a free-form option as a key/value pair.
     * <p>
     * This method is provided as a fallback if the SchemaBuilder is used with a more recent version of Cassandra that has new, unsupported options.
     *
     * @param key the name of the option.
     * @param value the value of the option. If it's a {@code String}, it will be included in single quotes, otherwise the result of invoking its
     *              {@code toString} method will be used unquoted.
     * @return this {@code TableOptions} object.
     */
    public T freeformOption(String key, Object value) {
        if (Strings.isNullOrEmpty(key)) {
            throw new IllegalArgumentException("Key for custom option should not be null or blank");
        }
        customOptions.add(new RawOption(key, value));
        return self;
    }

    List<String> buildCommonOptions() {
        List<String> options = new ArrayList<String>();

        buildCachingOptions(options);

        if (bloomFilterFPChance.isPresent()) {
            options.add("bloom_filter_fp_chance = " + bloomFilterFPChance.get());
        }

        if (comment.isPresent()) {
            options.add("comment = '" + comment.get() + "'");
        }

        if (compressionOptions.isPresent()) {
            options.add("compression = " + compressionOptions.get().build());
        }

        if (compactionOptions.isPresent()) {
            options.add("compaction = " + compactionOptions.get().build());
        }

        if (dcLocalReadRepairChance.isPresent()) {
            options.add("dclocal_read_repair_chance = " + dcLocalReadRepairChance.get());
        }

        if (defaultTTL.isPresent()) {
            options.add("default_time_to_live = " + defaultTTL.get());
        }

        if (gcGraceSeconds.isPresent()) {
            options.add("gc_grace_seconds = " + gcGraceSeconds.get());
        }

        if (indexInterval.isPresent()) {
            options.add("index_interval = " + indexInterval.get());
        }

        if (minIndexInterval.isPresent()) {
            options.add("min_index_interval = " + minIndexInterval.get());
        }

        if (maxIndexInterval.isPresent()) {
            options.add("max_index_interval = " + maxIndexInterval.get());
        }

        if (memtableFlushPeriodInMillis.isPresent()) {
            options.add("memtable_flush_period_in_ms = " + memtableFlushPeriodInMillis.get());
        }

        if (populateIOOnCacheFlush.isPresent()) {
            options.add("populate_io_cache_on_flush = " + populateIOOnCacheFlush.get());
        }

        if (readRepairChance.isPresent()) {
            options.add("read_repair_chance = " + readRepairChance.get());
        }

        if (replicateOnWrite.isPresent()) {
            options.add("replicate_on_write = " + replicateOnWrite.get());
        }

        if (speculativeRetry.isPresent()) {
            options.add("speculative_retry = " + speculativeRetry.get().value());
        }

        if (!customOptions.isEmpty()) {
            for (RawOption customOption : customOptions) {
                options.add(customOption.key + " = " + customOption.value);
            }
        }
        return options;
    }

    private void buildCachingOptions(List<String> options) {
        if (caching.isPresent()) {
            if (cachingRowsPerPartition.isPresent()) {
                final SchemaBuilder.Caching cachingType = caching.get();
                if ((cachingType == SchemaBuilder.Caching.ALL || cachingType == SchemaBuilder.Caching.NONE)) {
                    options.add("caching = {'keys' : " + caching.get().value() +
                        ", 'rows_per_partition' : " + cachingRowsPerPartition.get().value() + "}");
                } else {
                    throw new IllegalStateException("Cannot use caching type : " + cachingType.name() + " with the option 'rows_per_partition'. Please use ALL or NONE as caching type ");
                }
            } else {
                options.add("caching = " + caching.get());
            }
        }
    }

    String build() {
        return schemaStatement.buildInternal();
    }

    static void validateRateValue(Double rateValue, String property) {
        if (rateValue != null && (rateValue < 0 || rateValue > 1.0)) {
            throw new IllegalArgumentException(property + " should be between 0 and 1");
        }
    }

    /**
     * Compaction options for a CREATE or ALTER TABLE statement.
     * <p>
     * To create instances, use
     * {@link SchemaBuilder#sizedTieredStategy()},
     * {@link SchemaBuilder#leveledStrategy()} or
     * {@link SchemaBuilder#dateTieredStrategy()}
     */
    public static abstract class CompactionOptions<T extends CompactionOptions> {

        private Strategy strategy;

        private Optional<Boolean> enableBackgroundCompaction = Optional.absent();

        private Optional<Integer> tombstoneCompactionIntervalInDay = Optional.absent();

        private Optional<Double> tombstoneThreshold = Optional.absent();

        private Optional<Boolean> uncheckedTombstoneCompaction = Optional.absent();

        private T self;

        @SuppressWarnings("unchecked") CompactionOptions(Strategy compactionStrategy) {
            this.strategy = compactionStrategy;
            self = (T)this;
        }

        /**
         * Enables or disables background compaction
         * <p>
         *     If not set, default = true
         * </p>
         * @param enableBackgroundCompaction whether to enable background compaction for the table
         * @return this compaction options
         */
        public T enableBackgroundCompaction(Boolean enableBackgroundCompaction) {
            this.enableBackgroundCompaction = Optional.fromNullable(enableBackgroundCompaction);
            return self;
        }

        /**
         * The minimum number of days to wait after an SSTable creation time before considering the SSTable for tombstone compaction.
         * Tombstone compaction is the compaction triggered if the SSTable has more garbage-collectable tombstones than tombstone_threshold.
         * <p>
         *     If not set, default = 1 (day)
         * </p>
         * @param tombstoneCompactionInterval tombstone compaction interval in day.
         * @return this compaction options
         */
        public T tombstoneCompactionIntervalInDay(Integer tombstoneCompactionInterval) {
            this.tombstoneCompactionIntervalInDay = Optional.fromNullable(tombstoneCompactionInterval);
            return self;
        }

        /**
         * A ratio of garbage-collectable tombstones to all contained columns,
         * which if exceeded by the SSTable triggers compaction (with no other SSTables) for the purpose of purging the tombstones
         * <p>
         *     If not set, default = 0.2
         * </p>
         * @param tombstoneThreshold tombstone compaction interval in day.
         * @return this compaction options
         */
        public T tombstoneThreshold(Double tombstoneThreshold) {
            validateRateValue(tombstoneThreshold, "Tombstone threshold");
            this.tombstoneThreshold = Optional.fromNullable(tombstoneThreshold);
            return self;
        }

        /**
         * Enables more aggressive than normal tombstone compactions. A single SSTable tombstone compaction runs without
         * checking the likelihood of success.
         * <p>
         * If not set, default = false.
         * <p>
         * Cassandra 2.0.9 and later.
         *
         * @param uncheckedTombstoneCompaction whether to enable the feature.
         * @return this compaction options
         */
        public T uncheckedTombstoneCompaction(Boolean uncheckedTombstoneCompaction) {
            this.uncheckedTombstoneCompaction = Optional.fromNullable(uncheckedTombstoneCompaction);
            return self;
        }

        List<String> buildCommonOptions() {

            List<String> options = new ArrayList<String>();
            options.add("'class' : " + strategy.strategyClass());

            if (enableBackgroundCompaction.isPresent()) {
                options.add("'enabled' : " + enableBackgroundCompaction.get());
            }

            if (tombstoneCompactionIntervalInDay.isPresent()) {
                options.add("'tombstone_compaction_interval' : " + tombstoneCompactionIntervalInDay.get());
            }

            if (tombstoneThreshold.isPresent()) {
                options.add("'tombstone_threshold' : " + tombstoneThreshold.get());
            }

            if (uncheckedTombstoneCompaction.isPresent()) {
                options.add("'unchecked_tombstone_compaction' : " + uncheckedTombstoneCompaction.get());
            }

            return options;
        }

        public abstract String build();

        /**
         * Compaction options specific to SizeTiered strategy
         */
        public static class SizeTieredCompactionStrategyOptions extends CompactionOptions<SizeTieredCompactionStrategyOptions> {

            private Optional<Double> bucketHigh = Optional.absent();

            private Optional<Double> bucketLow = Optional.absent();

            private Optional<Double> coldReadsRatioToOmit = Optional.absent();

            private Optional<Integer> minThreshold = Optional.absent();

            private Optional<Integer> maxThreshold = Optional.absent();

            private Optional<Long> minSSTableSizeInBytes = Optional.absent();

            SizeTieredCompactionStrategyOptions() {
                super(Strategy.SIZED_TIERED);
            }

            /**
             * Size-tiered compaction strategy (STCS) considers SSTables to be within the same bucket if the SSTable size diverges by 50%
             * or less from the default bucket_low and default bucket_high values: [average-size × bucket_low, average-size × bucket_high].
             * <p>
             *     If not set, default = 1.5
             * </p>
             * @param bucketHigh bucket high
             * @return
             */
            public SizeTieredCompactionStrategyOptions bucketHigh(Double bucketHigh) {
                this.bucketHigh = Optional.fromNullable(bucketHigh);
                return this;
            }

            /**
             * Size-tiered compaction strategy (STCS) considers SSTables to be within the same bucket if the SSTable size diverges by 50%
             * or less from the default bucket_low and default bucket_high values: [average-size × bucket_low, average-size × bucket_high].
             * <p>
             *     If not set, default = 0.5
             * </p>
             * @param bucketLow bucket low
             * @return
             */
            public SizeTieredCompactionStrategyOptions bucketLow(Double bucketLow) {
                this.bucketLow = Optional.fromNullable(bucketLow);
                return this;
            }

            /**
             * The maximum percentage of reads/sec that ignored SSTables may account for.
             * The recommended range of values is 0.0 and 1.0.
             * In Cassandra 2.0.3 and later, you can enable the cold_reads_to_omit property to tune performace per table.
             * The <a href="http://www.datastax.com/dev/blog/optimizations-around-cold-sstables">Optimizations around Cold SSTables</a> blog includes detailed information tuning performance using this property,
             * which avoids compacting cold SSTables. Use the ALTER TABLE command to configure cold_reads_to_omit.
             * <p>
             *     If not set, default = 0.0 (disabled)
             * </p>
             * @param coldReadsRatio
             * @return
             */
            public SizeTieredCompactionStrategyOptions coldReadsRatioToOmit(Double coldReadsRatio) {
                validateRateValue(coldReadsRatio, "Cold read ratio to omit ");
                this.coldReadsRatioToOmit = Optional.fromNullable(coldReadsRatio);
                return this;
            }

            /**
             * Sets the minimum number of SSTables to trigger a minor compaction
             * <p>
             *     If not set, default = 4
             * </p>
             * @param minThreshold min threshold
             * @return
             */
            public SizeTieredCompactionStrategyOptions minThreshold(Integer minThreshold) {
                this.minThreshold = Optional.fromNullable(minThreshold);
                return this;
            }

            /**
             * Sets the maximum number of SSTables to allow in a minor compaction.
             * In LeveledCompactionStrategy (LCS), it applies to L0 when L0 gets behind, that is, when L0 accumulates more than MAX_COMPACTING_L0 SSTables.
             * <p>
             *     If not set, default = 32
             * </p>
             * @param maxThreshold max threshold
             * @return this compaction options
             */
            public SizeTieredCompactionStrategyOptions maxThreshold(Integer maxThreshold) {
                this.maxThreshold = Optional.fromNullable(maxThreshold);
                return this;
            }

            /**
             * The SizeTieredCompactionStrategy groups SSTables for compaction into buckets.
             * The bucketing process groups SSTables that differ in size by less than 50%. This results in a bucketing process that is too fine grained for small SSTables.
             * If your SSTables are small, use min_sstable_size to define a size threshold (in bytes) below which all SSTables belong to one unique bucket
             * <p>
             *     If not set, default = 52428800 (50Mb)
             * </p>
             * @param minSSTableSize min SSTable size in bytes
             * @return
             */
            public SizeTieredCompactionStrategyOptions minSSTableSizeInBytes(Long minSSTableSize) {
                this.minSSTableSizeInBytes = Optional.fromNullable(minSSTableSize);
                return this;
            }

            @Override
            public String build() {
                final List<String> generalOptions = super.buildCommonOptions();

                List<String> options = new ArrayList<String>(generalOptions);

                if (bucketHigh.isPresent()) {
                    options.add("'bucket_high' : " + bucketHigh.get());
                }

                if (bucketLow.isPresent()) {
                    options.add("'bucket_low' : " + bucketLow.get());
                }

                if (coldReadsRatioToOmit.isPresent()) {
                    options.add("'cold_reads_to_omit' : " + coldReadsRatioToOmit.get());
                }

                if (minThreshold.isPresent()) {
                    options.add("'min_threshold' : " + minThreshold.get());
                }

                if (maxThreshold.isPresent()) {
                    options.add("'max_threshold' : " + maxThreshold.get());
                }

                if (minSSTableSizeInBytes.isPresent()) {
                    options.add("'min_sstable_size' : " + minSSTableSizeInBytes.get());
                }
                return "{" + Joiner.on(", ").join(options) + "}";
            }
        }

        /**
         * Compaction options specific to Leveled strategy
         */
        public static class LeveledCompactionStrategyOptions extends CompactionOptions<LeveledCompactionStrategyOptions> {

            private Optional<Integer> ssTableSizeInMB = Optional.absent();

            LeveledCompactionStrategyOptions() {
                super(Strategy.LEVELED);
            }

            /**
             * The target size for SSTables that use the leveled compaction strategy.
             * Although SSTable sizes should be less or equal to sstable_size_in_mb, it is possible to have a larger SSTable during compaction.
             * This occurs when data for a given partition key is exceptionally large. The data is not split into two SSTables
             * <p>
             *     If not set, default = 160 Mb
             * </p>
             * @param ssTableSizeInMB  SSTable size in Mb
             * @return
             */
            public LeveledCompactionStrategyOptions ssTableSizeInMB(Integer ssTableSizeInMB) {
                this.ssTableSizeInMB = Optional.fromNullable(ssTableSizeInMB);
                return this;
            }

            @Override
            public String build() {
                final List<String> generalOptions = super.buildCommonOptions();

                List<String> options = new ArrayList<String>(generalOptions);

                if (ssTableSizeInMB.isPresent()) {
                    options.add("'sstable_size_in_mb' : " + ssTableSizeInMB.get());
                }
                return "{" + Joiner.on(", ").join(options) + "}";
            }

        }

        /**
         * Compaction options specific to the date-tiered strategy.
         */
        public static class DateTieredCompactionStrategyOptions extends CompactionOptions<DateTieredCompactionStrategyOptions> {

            enum TimeStampResolution {MICROSECONDS, MILLISECONDS}

            private Optional<Integer> baseTimeSeconds;

            private Optional<Integer> maxSSTableAgeDays;

            private Optional<Integer> minThreshold = Optional.absent();

            private Optional<Integer> maxThreshold = Optional.absent();

            private Optional<Long> minSSTableSizeInBytes = Optional.absent();

            private Optional<TimeStampResolution> timestampResolution = Optional.absent();

            DateTieredCompactionStrategyOptions() {
                super(Strategy.DATE_TIERED);
            }

            /**
             * Sets the size of the first window.
             * <p>
             * If not set, default = 3600 (1 hour).
             *
             * @param baseTimeSeconds the size of the first window.
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions baseTimeSeconds(Integer baseTimeSeconds) {
                this.baseTimeSeconds = Optional.fromNullable(baseTimeSeconds);
                return this;
            }

            /**
             * Stop compacting SSTables only having data older than these specified days.
             * <p>
             * If not set, default = 365.
             *
             * @param maxSSTableAgeDays the maximum age of the SSTables to compact.
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions maxSSTableAgeDays(Integer maxSSTableAgeDays) {
                this.maxSSTableAgeDays = Optional.fromNullable(maxSSTableAgeDays);
                return this;
            }

            /**
             * Sets the minimum number of SSTables to trigger a minor compaction
             * <p>
             *     If not set, default = 4
             * </p>
             * @param minThreshold min threshold
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions minThreshold(Integer minThreshold) {
                this.minThreshold = Optional.fromNullable(minThreshold);
                return this;
            }

            /**
             * Sets the maximum number of SSTables to allow in a minor compaction.
             * In LeveledCompactionStrategy (LCS), it applies to L0 when L0 gets behind, that is, when L0 accumulates more than MAX_COMPACTING_L0 SSTables.
             * <p>
             *     If not set, default = 32
             * </p>
             * @param maxThreshold max threshold
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions maxThreshold(Integer maxThreshold) {
                this.maxThreshold = Optional.fromNullable(maxThreshold);
                return this;
            }

            /**
             * If your SSTables are small, use min_sstable_size to define a size threshold (in bytes) below which all SSTables belong to one unique bucket.
             * <p>
             *     If not set, default = 52428800 (50Mb)
             * </p>
             * @param minSSTableSize min SSTable size in bytes
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions minSSTableSizeInBytes(Long minSSTableSize) {
                this.minSSTableSizeInBytes = Optional.fromNullable(minSSTableSize);
                return this;
            }

            /**
             * Sets the timestamp resolution, depending on the timestamp unit of the data you insert.
             *
             * @param timestampResolution {@link TimeStampResolution#MICROSECONDS} or {@link TimeStampResolution#MILLISECONDS}.
             * @return these compaction options.
             */
            public DateTieredCompactionStrategyOptions timestampResolution(TimeStampResolution timestampResolution) {
                this.timestampResolution = Optional.fromNullable(timestampResolution);
                return this;
            }

            @Override public String build() {
                final List<String> generalOptions = super.buildCommonOptions();

                List<String> options = new ArrayList<String>(generalOptions);

                if (baseTimeSeconds.isPresent()) {
                    options.add("'base_time_seconds' : " + baseTimeSeconds.get());
                }

                if (maxSSTableAgeDays.isPresent()) {
                    options.add("'max_sstable_age_days' : " + maxSSTableAgeDays.get());
                }

                if (minThreshold.isPresent()) {
                    options.add("'min_threshold' : " + minThreshold.get());
                }

                if (maxThreshold.isPresent()) {
                    options.add("'max_threshold' : " + maxThreshold.get());
                }

                if (minSSTableSizeInBytes.isPresent()) {
                    options.add("'min_sstable_size' : " + minSSTableSizeInBytes.get());
                }

                if (timestampResolution.isPresent()) {
                    options.add("'timestamp_resolution' : '" + timestampResolution.get() + "'");
                }

                return "{" + Joiner.on(", ").join(options) + "}";
            }
        }

        /**
         * Compaction strategies. Possible values: SIZED_TIERED, LEVELED & DATE_TIERED
         */
        public static enum Strategy {
            SIZED_TIERED("'SizeTieredCompactionStrategy'"), LEVELED("'LeveledCompactionStrategy'"), DATE_TIERED("'DateTieredCompactionStrategy'");

            private String strategyClass;

            Strategy(String strategyClass) {
                this.strategyClass = strategyClass;
            }

            public String strategyClass() {
                return strategyClass;
            }

            @Override
            public String toString() {
                return strategyClass;
            }
        }

    }

    /**
     * The compression options for a CREATE or ALTER TABLE statement.
     * <p>
     * To create instances, use
     * {@link SchemaBuilder#noCompression()},
     * {@link SchemaBuilder#lz4()},
     * {@link SchemaBuilder#snappy()} or
     * {@link SchemaBuilder#deflate()}.
     */
    public static class CompressionOptions {

        private Algorithm algorithm;

        private Optional<Integer> chunkLengthInKb = Optional.absent();

        private Optional<Double> crcCheckChance = Optional.absent();

        CompressionOptions(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        /**
         * On disk, SSTables are compressed by block to allow random reads.
         * This subproperty of compression defines the size (in KB) of the block.
         * Values larger than the default value might improve the compression rate, but increases the minimum size of data to be read from disk when a read occurs.
         * The default value is a good middle-ground for compressing tables.
         * Adjust compression size to account for read/write access patterns (how much data is typically requested at once) and the average size of rows in the table.
         * <p>
         *     If not set, default = 64kb
         * </p>
         * @param chunkLengthInKb chunk length in Kb
         * @return
         */
        public CompressionOptions withChunkLengthInKb(Integer chunkLengthInKb) {
            this.chunkLengthInKb = Optional.fromNullable(chunkLengthInKb);
            return this;
        }

        /**
         * When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrate and avoiding the propagation
         * of corruption to other replica. This option defines the probability with which those checksums are checked during read.
         * By default they are always checked. Set to 0 to disable checksum checking and to 0.5, for instance, to check them on every other read.
         * <p>
         *     If not set, default = 1.0 (always check)
         * </p>
         * @param crcCheckChance CRC check chance
         * @return
         */
        public CompressionOptions withCRCCheckChance(Double crcCheckChance) {
            validateRateValue(crcCheckChance, "CRC check chance");
            this.crcCheckChance = Optional.fromNullable(crcCheckChance);
            return this;
        }

        public String build() {
            List<String> options = new ArrayList<String>();
            options.add("'sstable_compression' : " + algorithm.value());

            if (chunkLengthInKb.isPresent()) {
                options.add("'chunk_length_kb' : " + chunkLengthInKb.get());
            }

            if (crcCheckChance.isPresent()) {
                options.add("'crc_check_chance' : " + crcCheckChance.get());
            }
            return "{" + Joiner.on(", ").join(options) + "}";
        }

        /**
         * Compression algorithms. Possible values: NONE, LZ4, SNAPPY, DEFLATE
         */
        public static enum Algorithm {
            NONE("''"), LZ4("'LZ4Compressor'"), SNAPPY("'SnappyCompressor'"), DEFLATE("'DeflateCompressor'");

            private String value;

            Algorithm(String value) {
                this.value = value;
            }

            public String value() {
                return value;
            }

            @Override
            public String toString() {
                return value;
            }
        }

        public static class NoCompression extends CompressionOptions {

            public NoCompression() {
                super(Algorithm.NONE);
            }

            public CompressionOptions withChunkLengthInKb(Integer chunkLengthInKb) {
                return this;
            }

            public CompressionOptions withCRCCheckChance(Double crcCheckChance) {
                return this;
            }
        }
    }

    /**
     * The speculative retry options.
     * <p>
     * To create instances, use
     * {@link SchemaBuilder#noSpeculativeRetry()},
     * {@link SchemaBuilder#always()},
     * {@link SchemaBuilder#percentile(int)} or
     * {@link SchemaBuilder#millisecs(int)}.
     */
    public static class SpeculativeRetryValue {

        private String value;

        SpeculativeRetryValue(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }
    }

    /**
     * Define the number of rows to be cached per partition when row caching is enabled
     * (this feature is only applicable to Cassandra 2.1.x).
     * <p>
     * To create instances, use
     * {@link SchemaBuilder#noRows()},
     * {@link SchemaBuilder#allRows()} or
     * {@link SchemaBuilder#rows(int)}.
     */
    public static class CachingRowsPerPartition {
        private String value;

        CachingRowsPerPartition(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    /**
     * For internal use only. Store a key/value of manual user-entered option
     */
    static class RawOption {
        final String key;
        final String value;

        RawOption(String key, Object value) {
            this.key = key;
            this.value = (value instanceof String)
                ? "'" + value + "'"
                : value.toString();
        }
    }
}
