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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

public class HudiTrinoRecordMerger implements HoodieRecordMerger {

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord hoodieRecord, Schema schema,
            HoodieRecord hoodieRecord1, Schema schema1, TypedProperties typedProperties)
            throws IOException {
        //TODO: implement this
        return null;
    }

    @Override
    public HoodieRecordType getRecordType() {
        return HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy() {
        return HoodieRecordMerger.DEFAULT_MERGE_STRATEGY_UUID;
    }
}
