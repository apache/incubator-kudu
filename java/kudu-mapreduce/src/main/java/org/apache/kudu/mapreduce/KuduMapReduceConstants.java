package org.apache.kudu.mapreduce;

public final class KuduMapReduceConstants {

  /** Job parameter that specifies the input table. */
  public static final String INPUT_TABLE_KEY = "kudu.mapreduce.input.table";
  /** Job parameter that specifies if the scanner should cache blocks or not (default: false). */
  public static final String SCAN_CACHE_BLOCKS = "kudu.mapreduce.input.scan.cache.blocks";
  /**
   * Job parameter that specifies if the scanner should be fault tolerant
   * or not (default: false).
   */
  public static final String FAULT_TOLERANT_SCAN = "kudu.mapreduce.input.fault.tolerant.scan";
  /** Job parameter that specifies where the masters are. */
  public static final String MASTER_ADDRESSES_KEY = "kudu.mapreduce.master.address";
  /** Job parameter that specifies how long we wait for operations to complete (default: 10s). */
  public static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";
  /** Job parameter that specifies the address for the name server. */
  public static final String NAME_SERVER_KEY = "kudu.mapreduce.name.server";
  /** Job parameter that specifies the encoded column predicates (may be empty). */
  public static final String ENCODED_PREDICATES_KEY =
      "kudu.mapreduce.encoded.predicates";
  /**
   * Job parameter that specifies the column projection as a comma-separated list of column names.
   *
   * Not specifying this at all (i.e. setting to null) or setting to the special string
   * '*' means to project all columns.
   *
   * Specifying the empty string means to project no columns (i.e just count the rows).
   */
  public static final String COLUMN_PROJECTION_KEY = "kudu.mapreduce.column.projection";
  /** Job parameter that specifies the output table. */
  public static final String OUTPUT_TABLE_KEY = "kudu.mapreduce.output.table";

  /** Number of rows that are buffered before flushing to the tablet server */
  public static final String BUFFER_ROW_COUNT_KEY = "kudu.mapreduce.buffer.row.count";

  /** default value for BUFFER_ROW_COUNT_KEY */
  public static final Integer DEFAULT_BUFFER_ROW_COUNT_KEY = 1000;

  /**
   * Job parameter that specifies which key is to be used to reach the KuduTableOutputFormat
   * belonging to the caller
   */
  public static final String MULTITON_KEY = "kudu.mapreduce.multitonkey";
}
