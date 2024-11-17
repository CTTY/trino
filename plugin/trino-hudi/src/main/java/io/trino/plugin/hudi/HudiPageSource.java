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

import static com.google.common.base.Preconditions.checkState;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

public class HudiPageSource implements ConnectorPageSource {

    HoodieFileGroupReader<IndexedRecord> fileGroupReader;
    // TODO: Remove pageSource here, Hudi doesn't use this page source to read
    ConnectorPageSource pageSource;
    HudiTrinoReaderContext readerContext;
    PageBuilder pageBuilder;
    HudiAvroSerializer avroSerializer;
    Map<Integer, String> partitionValueMap;

    public HudiPageSource(
            ConnectorPageSource pageSource,
            List<HivePartitionKey> partitionKeyList,
            HoodieFileGroupReader<IndexedRecord> fileGroupReader,
            HudiTrinoReaderContext readerContext,
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles) {
        this.pageSource = pageSource;
        this.fileGroupReader = fileGroupReader;
        this.initFileGroupReader();
        this.readerContext = readerContext;
        this.pageBuilder = new PageBuilder(dataHandles.stream().map(HiveColumnHandle::getType).toList());
        this.avroSerializer = new HudiAvroSerializer(columnHandles);
        Map<String, String> nameToPartitionValueMap = partitionKeyList.stream().collect(
                Collectors.toMap(HivePartitionKey::name, HivePartitionKey::value));
        this.partitionValueMap = new HashMap<>();
        for (int i = 0; i < dataHandles.size(); i++) {
            HiveColumnHandle handle = dataHandles.get(i);
            if (handle.isPartitionKey()) {
                partitionValueMap.put(i, nameToPartitionValueMap.get(handle.getName()));
            }
        }
    }

    @Override
    public long getCompletedBytes() {
        return pageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return pageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos() {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished() {
        try {
            return !fileGroupReader.hasNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page getNextPage() {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        try {
            while(fileGroupReader.hasNext()) {
                avroSerializer.buildRecordInPage(pageBuilder, fileGroupReader.next(), partitionValueMap, false);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Page newPage = pageBuilder.build();
        pageBuilder.reset();
        return newPage;
    }

    @Override
    public long getMemoryUsage() {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close() throws IOException {
        fileGroupReader.close();
        pageSource.close();
    }

    @Override
    public CompletableFuture<?> isBlocked() {
        return pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics() {
        return pageSource.getMetrics();
    }

    protected void initFileGroupReader() {
        try {
            this.fileGroupReader.initRecordIterators();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize file group reader!", e);
        }
    }
}
