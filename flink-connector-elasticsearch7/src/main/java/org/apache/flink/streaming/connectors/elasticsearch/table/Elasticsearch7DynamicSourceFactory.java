package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.StringUtils;

import java.time.ZoneId;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Internal
public class Elasticsearch7DynamicSourceFactory implements DynamicTableSourceFactory {

    private static final Set<ConfigOption<?>> requiredOptions =
            Stream.of(
                            ElasticsearchConnectorOptions.HOSTS_OPTION,
                            ElasticsearchConnectorOptions.INDEX_OPTION)
                    .collect(Collectors.toSet());
    private static final Set<ConfigOption<?>> optionalOptions =
            Stream.of(
                            ElasticsearchConnectorOptions.SCROLL_MAX_SIZE_OPTION,
                            ElasticsearchConnectorOptions.SCROLL_TIMEOUT_OPTION,
                            ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION,
                            ElasticsearchConnectorOptions.FAILURE_HANDLER_OPTION,
                            ElasticsearchConnectorOptions.FLUSH_ON_CHECKPOINT_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                            ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION,
                            ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX,
                            ElasticsearchConnectorOptions.FORMAT_OPTION,
                            ElasticsearchConnectorOptions.LOOKUP_CACHE_MAX_ROWS,
                            ElasticsearchConnectorOptions.LOOKUP_CACHE_TTL,
                            ElasticsearchConnectorOptions.LOOKUP_MAX_RETRIES,
                            ElasticsearchConnectorOptions.PASSWORD_OPTION,
                            ElasticsearchConnectorOptions.USERNAME_OPTION)
                    .collect(Collectors.toSet());

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        TableSchema schema = context.getCatalogTable().getSchema();
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        final DecodingFormat<DeserializationSchema<RowData>> format =
                helper.discoverDecodingFormat(
                        DeserializationFormatFactory.class,
                        ElasticsearchConnectorOptions.FORMAT_OPTION);
        helper.validate();
        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);
        Elasticsearch7Configuration config =
                new Elasticsearch7Configuration(configuration, context.getClassLoader());

        return new Elasticsearch7DynamicSource(
                format,
                config,
                TableSchemaUtils.getPhysicalSchema(schema),
                new ElasticsearchLookupOptions.Builder().build());
    }

    ZoneId getLocalTimeZoneId(ReadableConfig readableConfig) {
        final String zone = readableConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);
        final ZoneId zoneId =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zone);

        return zoneId;
    }

    private void validate(Elasticsearch7Configuration config, Configuration originalConfiguration) {
        config.getFailureHandler(); // checks if we can instantiate the custom failure handler
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () ->
                        String.format(
                                "'%s' must not be empty",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
                maxActions == -1 || maxActions >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(),
                                maxActions));
        long maxSize = config.getBulkFlushMaxByteSize();
        long mb1 = 1024 * 1024;
        validate(
                maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
                () ->
                        String.format(
                                "'%s' must be in MB granularity. Got: %s",
                                ElasticsearchConnectorOptions.BULK_FLASH_MAX_SIZE_OPTION.key(),
                                originalConfiguration
                                        .get(
                                                ElasticsearchConnectorOptions
                                                        .BULK_FLASH_MAX_SIZE_OPTION)
                                        .toHumanReadableString()));
        validate(
                config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION
                                        .key(),
                                config.getBulkFlushBackoffRetries().get()));
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                    config.getPassword().isPresent()
                            && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                    () ->
                            String.format(
                                    "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                                    ElasticsearchConnectorOptions.USERNAME_OPTION.key(),
                                    ElasticsearchConnectorOptions.PASSWORD_OPTION.key(),
                                    config.getUsername().get(),
                                    config.getPassword().orElse("")));
        }
    }

    private void validateSource(
            Elasticsearch7Configuration config, Configuration originalConfiguration) {
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () ->
                        String.format(
                                "'%s' must not be empty",
                                ElasticsearchConnectorOptions.INDEX_OPTION.key()));
        validate(
                config.getScrollMaxSize().map(scrollMaxSize -> scrollMaxSize >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.SCROLL_MAX_SIZE_OPTION.key(),
                                config.getScrollMaxSize().get()));
        validate(
                config.getScrollTimeout().map(scrollTimeout -> scrollTimeout >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.SCROLL_TIMEOUT_OPTION.key(),
                                config.getScrollTimeout().get()));
        long cacheMaxSize = config.getCacheMaxSize();
        validate(
                cacheMaxSize == -1 || cacheMaxSize >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.LOOKUP_CACHE_MAX_ROWS.key(),
                                cacheMaxSize));
        validate(
                config.getCacheExpiredMs().getSeconds() >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.LOOKUP_CACHE_TTL.key(),
                                config.getCacheExpiredMs().getSeconds()));
        validate(
                config.getMaxRetryTimes() >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                ElasticsearchConnectorOptions.LOOKUP_MAX_RETRIES.key(),
                                config.getMaxRetryTimes()));
    }

    private static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    @Override
    public String factoryIdentifier() {
        return "elasticsearch-7";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }
}
