package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.shaded.curator5.com.google.common.base.Preconditions.checkNotNull;

/**
 * 输入反序列化器
 *
 * @param <T>
 * @param <C>
 */
@Internal
public class ElasticSearchInputFormatBase<T, C extends AutoCloseable>
        extends RichInputFormat<T, ElasticsearchInputSplit> implements ResultTypeQueryable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchInputFormatBase.class);
    private static final long serialVersionUID = 1L;
    private final DeserializationSchema<T> deserializationSchema;
    private final String index;
    private final String type;

    private final String[] fieldNames;
    private final QueryBuilder predicate;
    private final long limit;

    private final long scrollTimeout;
    private final int scrollSize;

    private Scroll scroll;
    private String currentScrollWindowId;
    private String[] currentScrollWindowHits;

    private int nextRecordIndex = 0;
    private long currentReadCount = 0L;

    private final ElasticsearchApiCallBridge<C> callBridge;

    private final Map<String, String> userConfig;

    private transient C client;

    public ElasticSearchInputFormatBase(
            ElasticsearchApiCallBridge<C> callBridge,
            Map<String, String> userConfig,
            DeserializationSchema<T> deserializationSchema,
            String[] fieldNames,
            String index,
            String type,
            long scrollTimeout,
            int scrollSize,
            QueryBuilder predicate,
            long limit) {

        this.callBridge = checkNotNull(callBridge);
        checkNotNull(userConfig);
        this.userConfig = new HashMap<>(userConfig);

        this.deserializationSchema = checkNotNull(deserializationSchema);
        this.index = index;
        this.type = type;

        this.fieldNames = fieldNames;
        this.predicate = predicate;
        this.limit = limit;

        this.scrollTimeout = scrollTimeout;
        this.scrollSize = scrollSize;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    @Override
    public void configure(Configuration configuration) {

        try {
            client = callBridge.createClient();
            callBridge.verifyClientConnection(client);
        } catch (IOException ex) {
            LOG.error("Exception while creating connection to Elasticsearch.", ex);
            throw new RuntimeException("Cannot create connection to Elasticsearcg.", ex);
        }
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public ElasticsearchInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        return callBridge.createInputSplitsInternal(client, index, type, minNumSplits);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(ElasticsearchInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(ElasticsearchInputSplit split) throws IOException {
        // 初始化反序列化器
        try {
            deserializationSchema.open(null);
        } catch (Exception e) {
            e.printStackTrace();
        }

        SearchRequest searchRequest = new SearchRequest(index);
        if (type == null) {
            searchRequest.types(Strings.EMPTY_ARRAY);
        } else {
            searchRequest.types(type);
        }

        // 构建查询请求
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        int size = 10;
        if (limit > 0) {
            size = (int) Math.min(limit, scrollSize);
        } else {
            size = scrollSize;
        }

        // 设置每批查询记录数
        searchSourceBuilder.size(size);

        // 滚动查询请求
        searchRequest.source(searchSourceBuilder);

        // 设置查询返回字段
        searchSourceBuilder.fetchSource(fieldNames, null);

        // 设置请求滚动时间窗口时间
        this.scroll = new Scroll(TimeValue.timeValueMinutes(scrollTimeout));
        searchRequest.scroll(scroll);

        if (predicate != null) {
            searchSourceBuilder.query(predicate);
        } else {
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        }

        Tuple2<String, String[]> searchResponse = null;
        try {
            searchResponse = callBridge.search(client, searchRequest);
        } catch (IOException e) {
            LOG.error("Search has error: ", e.getMessage());
        }
        if (searchResponse != null) {
            currentScrollWindowId = searchResponse.f0;
            currentScrollWindowHits = searchResponse.f1;
            nextRecordIndex = 0;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (limit > 0 && currentReadCount >= limit) {
            return true;
        }
        if (currentScrollWindowHits.length != 0
                && nextRecordIndex > currentScrollWindowHits.length - 1) {
            fetchNextScrollWindow();
        }

        return currentScrollWindowHits.length == 0;
    }

    @Override
    public T nextRecord(T t) throws IOException {
        // 拉取记录
        if (reachedEnd()) {
            LOG.warn("Already reached the end of the split.");
        }

        String hit = currentScrollWindowHits[nextRecordIndex];
        nextRecordIndex++;
        currentReadCount++;
        LOG.debug("Yielding new record for hit: " + hit);

        // 反序列化每条记录
        return parseSearchHit(hit);
    }

    @Override
    public void close() throws IOException {
        callBridge.close(client);
    }

    private void fetchNextScrollWindow() {
        // 滚动查询
        Tuple2<String, String[]> searchResponse = null;
        SearchScrollRequest scrollRequest = new SearchScrollRequest(currentScrollWindowId);
        scrollRequest.scroll(scroll);

        try {
            searchResponse = callBridge.scroll(client, scrollRequest);
        } catch (IOException e) {
            LOG.error("Scroll failed: " + e.getMessage());
        }

        if (searchResponse != null) {
            currentScrollWindowId = searchResponse.f0;
            currentScrollWindowHits = searchResponse.f1;
            nextRecordIndex = 0;
        }

        // 清理滚动上下文
        if (StringUtils.isNotBlank(currentScrollWindowId) && currentScrollWindowHits.length == 0) {
            try {
                callBridge.clearContext(client, currentScrollWindowId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private T parseSearchHit(String hit) {
        LOG.error("反序列化数据为：" + hit);
        T row = null;
        try {
            row = deserializationSchema.deserialize(hit.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            LOG.error("Deserialize search hit failed: " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return row;
    }
}
