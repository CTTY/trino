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
package io.trino.plugin.hudi.query;

import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry.Block;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.storage.StoragePathInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private static final long MIN_BLOCK_SIZE = DataSize.of(32, MEGABYTE).toBytes();

    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;
    private final Map<String, HudiPartitionInfo> allPartitionInfoMap;

    public HudiReadOptimizedDirectoryLister(
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> hivePartitionNames)
    {
        this.fileSystemView = new HoodieMetadataFileSystemView(new HoodieLocalEngineContext(new TrinoStorageConfiguration()),
                metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants(),
                HoodieMetadataConfig.newBuilder().build());
                //new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
        this.partitionColumns = hiveTable.getPartitionColumns();
        this.allPartitionInfoMap = hivePartitionNames.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        hivePartitionName -> new HiveHudiPartitionInfo(
                                hivePartitionName,
                                partitionColumns,
                                partitionColumnHandles,
                                tableHandle.getPartitionPredicates(),
                                hiveTable,
                                hiveMetastore)));
    }

    @Override
    public List<HudiFileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        List<HoodieBaseFile> collect = fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath()).toList();

        return fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(HudiReadOptimizedDirectoryLister::getStoragePathInfo)
                .map(fileEntry -> new HudiFileStatus(
                        Location.of(fileEntry.getPath().toString()),
                        false,
                        fileEntry.getLength(),
                        fileEntry.getModificationTime(),
                        max(fileEntry.getBlockSize(), min(fileEntry.getLength(), MIN_BLOCK_SIZE))))
                .collect(toImmutableList());
    }

    @Override
    public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
    {
        return Optional.ofNullable(allPartitionInfoMap.get(partition));
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }

    private static long blockSize(Optional<List<Block>> blocks)
    {
        return blocks.stream()
                .flatMap(Collection::stream)
                .mapToLong(Block::length)
                .findFirst()
                .orElse(0);
    }

    private static StoragePathInfo getStoragePathInfo(HoodieBaseFile baseFile) {
        if (baseFile.getBootstrapBaseFile().isPresent()) {
            return baseFile.getBootstrapBaseFile().get().getPathInfo();
        }
        return baseFile.getPathInfo();
    }
}
