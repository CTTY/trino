/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TypeSignature;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.predicate.PredicateUtils.buildPredicate;
import static io.trino.parquet.predicate.PredicateUtils.getFilteredRowGroups;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.ParquetReaderProvider;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createParquetPageSource;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetMessageType;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CURSOR_ERROR;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_FILE_FORMAT;
import static io.trino.plugin.hudi.HudiSessionProperties.getParquetSmallFileThreshold;
import static io.trino.plugin.hudi.HudiSessionProperties.isParquetVectorizedDecodingEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.shouldUseParquetColumnNames;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;
import static io.trino.plugin.hudi.HudiUtil.convertToFileSlice;
import static io.trino.plugin.hudi.HudiUtil.getHudiFileFormat;
import static io.trino.plugin.hudi.HudiUtil.prependHudiMetaColumns;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class HudiPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats dataSourceStats;
    private final ParquetReaderOptions options;
    private final DateTimeZone timeZone;
    private final boolean useUniPageSource;
    private static final int DOMAIN_COMPACTION_THRESHOLD = 1000;

    @Inject
    public HudiPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderConfig parquetReaderConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.dataSourceStats = requireNonNull(dataSourceStats, "dataSourceStats is null");
        this.options = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.timeZone = DateTimeZone.forID(TimeZone.getDefault().getID());
        this.useUniPageSource = true;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (useUniPageSource) {
            HudiTableHandle hudiTableHandle = (HudiTableHandle) connectorTable;
            HudiSplit hudiSplit = (HudiSplit) connectorSplit;
            Optional<HudiBaseFile> hudiBaseFileOpt = hudiSplit.getBaseFile();
            long start = 0;
            long length = 10;
            if (hudiBaseFileOpt.isPresent()) {
                start = hudiBaseFileOpt.get().getStart();
                length = hudiBaseFileOpt.get().getLength();
            }
            HoodieTableMetaClient metaClient = buildTableMetaClient(fileSystemFactory.create(session), hudiTableHandle.getBasePath());
            String latestCommitTime = metaClient.getCommitsTimeline().lastInstant().get().getTimestamp();
            Schema dataSchema = null;
            try {
                dataSchema = new TableSchemaResolver(metaClient).getTableAvroSchema(latestCommitTime);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            List<HiveColumnHandle> hiveColumns = columns.stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toList());

            List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                    .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                    .collect(Collectors.toList());
            List<HiveColumnHandle> columnHandles = prependHudiMetaColumns(regularColumns);

            Schema requestedSchema = constructSchema(columnHandles.stream().map(HiveColumnHandle::getName).toList(),
                    columnHandles.stream().map(HiveColumnHandle::getHiveType).toList(), false);

            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            ConnectorPageSource dataPageSource = createPageSource(
                    session,
                    regularColumns,
                    hudiSplit,
                    fileSystem.newInputFile(Location.of(hudiBaseFileOpt.get().getPath()), hudiBaseFileOpt.get().getFileSize()),
                    dataSourceStats,
                    options.withSmallFileThreshold(getParquetSmallFileThreshold(session))
                            .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session)),
                    timeZone);

            HudiTrinoReaderContext readerContext = new HudiTrinoReaderContext(
                    dataPageSource,
                    hudiSplit.getPartitionKeys(),
                    hiveColumns.stream()
                            .filter(columnHandle -> !columnHandle.isHidden())
                            .collect(Collectors.toList()),
                    prependHudiMetaColumns(regularColumns));

            HoodieFileGroupReader<IndexedRecord> fileGroupReader =
                    new HoodieFileGroupReader<>(
                            readerContext,
                            new HudiTrinoStorage(fileSystemFactory.create(session), new TrinoStorageConfiguration()),
                            hudiTableHandle.getBasePath(),
                            latestCommitTime,
                            convertToFileSlice(hudiSplit, hudiTableHandle.getBasePath()),
                            dataSchema,
                            requestedSchema,
                            Option.empty(),
                            metaClient,
                            metaClient.getTableConfig().getProps(),
                            start,
                            length,
                            false);

            return new HudiPageSource(
                    dataPageSource,
                    hudiSplit.getPartitionKeys(),
                    fileGroupReader,
                    readerContext,
                    hiveColumns.stream()
                            .filter(columnHandle -> !columnHandle.isHidden())
                            .collect(Collectors.toList()),
                    prependHudiMetaColumns(regularColumns)
            );
        }

        HudiSplit split = (HudiSplit) connectorSplit;
        String dataFilePath = split.getBaseFile().isPresent()
                ? split.getBaseFile().get().getPath()
                : split.getLogFiles().get(0);
        // Filter out metadata table splits
        if (dataFilePath.contains(new StoragePath(
                ((HudiTableHandle) connectorTable).getBasePath()).toUri().getPath() + "/.hoodie/metadata")) {
            return new EmptyPageSource();
        }
        if (split.getLogFiles().isEmpty()) {
            HudiBaseFile baseFile = split.getBaseFile().get();
            String path = baseFile.getPath();
            HoodieFileFormat hudiFileFormat = getHudiFileFormat(path);
            if (!HoodieFileFormat.PARQUET.equals(hudiFileFormat)) {
                throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, format("File format %s not supported", hudiFileFormat));
            }

            List<HiveColumnHandle> hiveColumns = columns.stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toList());
            // just send regular columns to create parquet page source
            // for partition columns, separate blocks will be created
            List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                    .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                    .collect(Collectors.toList());
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path), baseFile.getFileSize());
            ConnectorPageSource dataPageSource = createPageSource(
                    session,
                    regularColumns,
                    split,
                    inputFile,
                    dataSourceStats,
                    options.withSmallFileThreshold(getParquetSmallFileThreshold(session))
                            .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session)),
                    timeZone);

            return new HudiReadOptimizedPageSource(
                    toPartitionName(split.getPartitionKeys()),
                    hiveColumns,
                    convertPartitionValues(hiveColumns, split.getPartitionKeys()), // create blocks for partition values
                    dataPageSource,
                    path,
                    baseFile.getFileSize(),
                    baseFile.getModificationTime());
        }

        HudiTableHandle hudiTableHandle = (HudiTableHandle) connectorTable;
        HudiBaseFile baseFile = split.getBaseFile().get();
        String path = baseFile.getPath();
        HoodieFileFormat hudiFileFormat = getHudiFileFormat(path);
        if (!HoodieFileFormat.PARQUET.equals(hudiFileFormat)) {
            throw new TrinoException(HUDI_UNSUPPORTED_FILE_FORMAT, format("File format %s not supported", hudiFileFormat));
        }

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        // just send regular columns to create parquet page source
        // for partition columns, separate blocks will be created
        List<HiveColumnHandle> regularColumns = hiveColumns.stream()
                .filter(columnHandle -> !columnHandle.isPartitionKey() && !columnHandle.isHidden())
                .collect(Collectors.toList());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path), baseFile.getFileSize());
        ConnectorPageSource dataPageSource = createPageSource(
                session,
                prependHudiMetaColumns(regularColumns),
                split,
                inputFile,
                dataSourceStats,
                options.withSmallFileThreshold(getParquetSmallFileThreshold(session))
                        .withVectorizedDecodingEnabled(isParquetVectorizedDecodingEnabled(session)),
                timeZone);
        return new HudiSnapshotPageSource(
                split.getPartitionKeys(),
                new HudiTrinoStorage(fileSystemFactory.create(session), new TrinoStorageConfiguration()),
                hudiTableHandle.getBasePath(),
                split,
                Optional.of(dataPageSource),
                hiveColumns.stream()
                        .filter(columnHandle -> !columnHandle.isHidden())
                        .collect(Collectors.toList()),
                prependHudiMetaColumns(regularColumns),
                hudiTableHandle.getPreCombineField());
    }

    private static ConnectorPageSource createPageSource(
            ConnectorSession session,
            List<HiveColumnHandle> columns,
            HudiSplit hudiSplit,
            TrinoInputFile inputFile,
            FileFormatDataSourceStats dataSourceStats,
            ParquetReaderOptions options,
            DateTimeZone timeZone)
    {
        ParquetDataSource dataSource = null;
        boolean useColumnNames = shouldUseParquetColumnNames(session);
        HudiBaseFile baseFile = hudiSplit.getBaseFile().get();
        String path = baseFile.getPath();
        long start = baseFile.getStart();
        long length = baseFile.getLength();
        try {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            dataSource = createDataSource(inputFile, OptionalLong.of(baseFile.getFileSize()), options, memoryContext, dataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = getParquetMessageType(columns, useColumnNames, fileSchema);

            MessageType requestedSchema = message.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            MessageColumnIO messageColumn = getColumnIO(fileSchema, requestedSchema);

            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = options.isIgnoreStatistics()
                    ? TupleDomain.all()
                    : getParquetTupleDomain(descriptorsByPath, hudiSplit.getPredicate(), fileSchema, useColumnNames);

            TupleDomainParquetPredicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, timeZone);

            List<RowGroupInfo> rowGroups = getFilteredRowGroups(
                    start,
                    length,
                    dataSource,
                    parquetMetadata.getBlocks(),
                    ImmutableList.of(parquetTupleDomain),
                    ImmutableList.of(parquetPredicate),
                    descriptorsByPath,
                    timeZone,
                    DOMAIN_COMPACTION_THRESHOLD,
                    options);

            Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);
            List<HiveColumnHandle> baseColumns = readerProjections.map(projection ->
                            projection.get().stream()
                                    .map(HiveColumnHandle.class::cast)
                                    .collect(toUnmodifiableList()))
                    .orElse(columns);
            ParquetDataSourceId dataSourceId = dataSource.getId();
            ParquetDataSource finalDataSource = dataSource;
            ParquetReaderProvider parquetReaderProvider = fields -> new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    fields,
                    rowGroups,
                    finalDataSource,
                    timeZone,
                    memoryContext,
                    options,
                    exception -> handleException(dataSourceId, exception),
                    Optional.of(parquetPredicate),
                    Optional.empty());
            return createParquetPageSource(baseColumns, fileSchema, messageColumn, useColumnNames, parquetReaderProvider);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof TrinoException) {
                throw (TrinoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new TrinoException(HUDI_BAD_DATA, e);
            }
            String message = "Error opening Hudi split %s (offset=%s, length=%s): %s".formatted(path, start, length, e.getMessage());
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(HUDI_BAD_DATA, exception);
        }
        return new TrinoException(HUDI_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    private Map<String, Block> convertPartitionValues(
            List<HiveColumnHandle> allColumns,
            List<HivePartitionKey> partitionKeys)
    {
        return allColumns.stream()
                .filter(HiveColumnHandle::isPartitionKey)
                .collect(toMap(
                        HiveColumnHandle::getName,
                        columnHandle -> nativeValueToBlock(
                                columnHandle.getType(),
                                partitionToNativeValue(
                                        columnHandle.getName(),
                                        partitionKeys,
                                        columnHandle.getType().getTypeSignature()).orElse(null))));
    }

    private static Optional<Object> partitionToNativeValue(
            String partitionColumnName,
            List<HivePartitionKey> partitionKeys,
            TypeSignature partitionDataType)
    {
        HivePartitionKey partitionKey = partitionKeys.stream().filter(key -> key.name().equalsIgnoreCase(partitionColumnName)).findFirst().orElse(null);
        if (isNull(partitionKey)) {
            return Optional.empty();
        }

        String partitionValue = partitionKey.value();
        String baseType = partitionDataType.getBase();
        try {
            return switch (baseType) {
                case TINYINT, SMALLINT, INTEGER, BIGINT -> Optional.of(parseLong(partitionValue));
                case REAL -> Optional.of((long) floatToRawIntBits(parseFloat(partitionValue)));
                case DOUBLE -> Optional.of(parseDouble(partitionValue));
                case VARCHAR, VARBINARY -> Optional.of(utf8Slice(partitionValue));
                case DATE -> Optional.of(LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay());
                case TIMESTAMP -> Optional.of(Timestamp.valueOf(partitionValue).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000);
                case BOOLEAN -> {
                    checkArgument(partitionValue.equalsIgnoreCase("true") || partitionValue.equalsIgnoreCase("false"));
                    yield Optional.of(Boolean.valueOf(partitionValue));
                }
                case DECIMAL -> Optional.of(Decimals.parse(partitionValue).getObject());
                default -> throw new TrinoException(
                        HUDI_INVALID_PARTITION_VALUE,
                        format("Unsupported data type '%s' for partition column %s", partitionDataType, partitionColumnName));
            };
        }
        catch (IllegalArgumentException | DateTimeParseException e) {
            throw new TrinoException(
                    HUDI_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s'", partitionValue, partitionDataType, partitionColumnName),
                    e);
        }
    }

    private static String toPartitionName(List<HivePartitionKey> partitions)
    {
        ImmutableList.Builder<String> partitionNames = ImmutableList.builderWithExpectedSize(partitions.size());
        ImmutableList.Builder<String> partitionValues = ImmutableList.builderWithExpectedSize(partitions.size());
        for (HivePartitionKey partition : partitions) {
            partitionNames.add(partition.name());
            partitionValues.add(partition.value());
        }
        return makePartName(partitionNames.build(), partitionValues.build());
    }
}
