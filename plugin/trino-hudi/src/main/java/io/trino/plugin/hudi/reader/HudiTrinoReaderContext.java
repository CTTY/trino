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
package io.trino.plugin.hudi.reader;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

public class HudiTrinoReaderContext extends HoodieReaderContext<IndexedRecord> {

    ConnectorPageSource pageSource;
    private HudiAvroSerializer avroSerializer;
    private PageBuilder pageBuilder;
    Map<Integer, String> partitionValueMap;
    Map<String, Integer> colToPosMap;
    List<HiveColumnHandle> columnHandles;

    public HudiTrinoReaderContext(
            ConnectorPageSource pageSource,
            List<HivePartitionKey> partitionKeyList,
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles) {
        this.pageSource = pageSource;
        this.avroSerializer = new HudiAvroSerializer(columnHandles);
        this.pageBuilder = new PageBuilder(dataHandles.stream().map(HiveColumnHandle::getType).toList());
        Map<String, String> nameToPartitionValueMap = partitionKeyList.stream().collect(
                Collectors.toMap(e -> e.name(), e -> e.value()));
        this.partitionValueMap = new HashMap<>();
        for (int i = 0; i < dataHandles.size(); i++) {
            HiveColumnHandle handle = dataHandles.get(i);
            if (handle.isPartitionKey()) {
                partitionValueMap.put(i + HOODIE_META_COLUMNS.size(), nameToPartitionValueMap.get(handle.getName()));
            }
        }

        this.columnHandles = columnHandles;
        for (int i = 0; i < columnHandles.size(); i++) {
            HiveColumnHandle handle = columnHandles.get(i);
            colToPosMap.put(handle.getBaseColumnName(), i);
        }
    }

    @Override
    public HoodieStorage getStorage(String path, StorageConfiguration<?> storageConf) {
        return HoodieStorageUtils.getStorage(path, storageConf);
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePath storagePath,
            long start,
            long length,
            Schema dataSchema,
            Schema requiredSchema,
            HoodieStorage storage) {
        Page baseFilePage = pageSource.getNextPage();
        return new ClosableIterator<>() {
            int pos = 0;

            @Override
            public void close() {
                try {
                    pageSource.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return pos < baseFilePage.getPositionCount();
            }

            @Override
            public IndexedRecord next() {
                IndexedRecord record = avroSerializer.serialize(baseFilePage, pos);
                pos++;
                return record;
            }
        };
    }

    @Override
    public IndexedRecord convertAvroRecord(IndexedRecord record) {
        return record;
    }

    @Override
    public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
        return HoodieAvroRecordMerger.INSTANCE;
    }

    @Override
    public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
        return record.get(colToPosMap.get(fieldName));
    }

    @Override
    public IndexedRecord seal(IndexedRecord record) {
        // TODO: this can rely on colToPos map directly instead of schema
        Schema schema = record.getSchema();
        IndexedRecord newRecord = new Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            int pos = schema.getField(field.name()).pos();
            newRecord.put(pos, record.get(pos));
        }
        return newRecord;
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(
            ClosableIterator closableIterator, Schema schema,
            ClosableIterator closableIterator1, Schema schema1) {
        return null;
    }

    @Override
    public UnaryOperator<IndexedRecord> projectRecord(
            Schema from,
            Schema to,
            Map<String, String> renamedColumns) {
        List<Schema.Field> toFields = to.getFields();
        int[] projection = new int[toFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = from.getField(toFields.get(i).name()).pos();
        }

        return fromRecord -> {
            IndexedRecord toRecord = new Record(to);
            for (int i = 0; i < projection.length; i++) {
                toRecord.put(i, fromRecord.get(projection[i]));
            }
            return toRecord;
        };
    }

    @Override
    public HoodieRecord<IndexedRecord> constructHoodieRecord(
            Option<IndexedRecord> recordOpt,
            Map<String, Object> metadataMap) {
        if (!recordOpt.isPresent()) {
            return new HoodieEmptyRecord<>(
                    new HoodieKey((String) metadataMap.get(INTERNAL_META_RECORD_KEY),
                            (String) metadataMap.get(INTERNAL_META_PARTITION_PATH)),
                    HoodieRecord.HoodieRecordType.AVRO);
        }
        return new HoodieAvroIndexedRecord(recordOpt.get());
    }
}
