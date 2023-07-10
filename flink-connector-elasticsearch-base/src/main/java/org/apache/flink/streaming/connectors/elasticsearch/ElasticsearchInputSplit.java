package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.core.io.LocatableInputSplit;

public class ElasticsearchInputSplit extends LocatableInputSplit {

    private static final long seriaVersionUID = 1L;

    /** 索引 */
    private final String index;

    /** type elasticsearch7 之后已弃用 */
    private final String type;

    /** 分片 */
    private final int shard;

    public ElasticsearchInputSplit(
            int splitNumber, String[] hostnames, String index, String type, int shard) {
        super(splitNumber, hostnames);
        this.index = index;
        this.type = type;
        this.shard = shard;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    public int getShard() {
        return shard;
    }
}
