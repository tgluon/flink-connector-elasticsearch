package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticSearchInputFormatBase;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.HashMap;
import java.util.Map;

@PublicEvolving
public class ElasticSearch7InputFormat<T>
        extends ElasticSearchInputFormatBase<T, RestHighLevelClient> {

    private static final long serialVersionUID = 1L;

    public ElasticSearch7InputFormat(
            Map<String, String> userConfig,
            String httpHosts,
            RestClientFactory restClientFactory,
            DeserializationSchema<T> serializationSchema,
            String[] fieldNames,
            String index,
            long scrollTimeout,
            int scrollSize,
            QueryBuilder predicate,
            int limit) {

        super(
                new Elasticsearch7ApiCallBridge(httpHosts, restClientFactory),
                userConfig,
                serializationSchema,
                fieldNames,
                index,
                null,
                scrollTimeout,
                scrollSize,
                predicate,
                limit);
    }

    @PublicEvolving
    public static class Builder<T> {
        private Map<String, String> userConfig = new HashMap<>();
        private String httpHosts;
        private RestClientFactory restClientFactory = restClientBuilder -> {};
        private DeserializationSchema<T> deserializationSchema;
        private String index;

        private long scrollTimeout;
        private int scrollMaxSize;

        private String[] fieldNames;
        private QueryBuilder predicate;
        private int limit;

        public Builder() {}

        public Builder setHttpHosts(String httpHosts) {
            this.httpHosts = httpHosts;
            return this;
        }

        public Builder setRestClientFactory(RestClientFactory restClientFactory) {
            this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
            return this;
        }

        public Builder setDeserializationSchema(DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        public Builder setIndex(String index) {
            this.index = index;
            return this;
        }

        /**
         * 滚动查询每批记录数
         *
         * @param scrollMaxSize
         * @return
         */
        public Builder setScrollMaxSize(int scrollMaxSize) {
            Preconditions.checkArgument(
                    scrollMaxSize > 0,
                    "Maximum number each Elasticsearch scroll request must be larger than 0.");

            this.scrollMaxSize = scrollMaxSize;
            return this;
        }

        /**
         * 滚动查询超时时间
         *
         * @param scrollTimeout
         * @return
         */
        public Builder setScrollTimeout(long scrollTimeout) {
            Preconditions.checkArgument(
                    scrollTimeout >= 0,
                    "Yhe search context alive for scroll requests must be larger than or equal to 0.");

            this.scrollTimeout = scrollTimeout;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setPredicate(QueryBuilder predicate) {
            this.predicate = predicate;
            return this;
        }

        public Builder setLimit(int limit) {
            this.limit = limit;
            return this;
        }

        public ElasticSearch7InputFormat<T> build() {
            return new ElasticSearch7InputFormat<T>(
                    userConfig,
                    httpHosts,
                    restClientFactory,
                    deserializationSchema,
                    fieldNames,
                    index,
                    scrollTimeout,
                    scrollMaxSize,
                    predicate,
                    limit);
        }
    }
}
