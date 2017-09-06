/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import java.util.stream.Collectors;

import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

public class ManagedCursorSizeTest {

    @DataProvider(name = "maxUnackedRangesToPersist")
    public Object[][] maxUnackedRangesToPersist() {
        return new Object[][] { { 0 }, { 10 }, { 1000 } };
    }

    @Test(dataProvider = "maxUnackedRangesToPersist")
    private void printMLCursorSize(int maxUnackedRangesToPersist) {
        long cursorsLedgerId = 459991234L;
        long ledgerId = 459991234L;
        int entryId = 123;
        ManagedCursorInfo.Builder info = ManagedCursorInfo.newBuilder() //
                .setCursorsLedgerId(cursorsLedgerId) //
                .setMarkDeleteLedgerId(ledgerId) //
                .setMarkDeleteEntryId(entryId); //

        info.addAllIndividualDeletedMessages(buildIndividualDeletedMessageRanges(maxUnackedRangesToPersist));
        ManagedCursorInfo cursor = info.build();

        byte[] contentText = cursor.toString().getBytes(Charsets.UTF_8);
        byte[] contentBinary = cursor.toByteArray();

        System.out.println("Total-markDeleteRange = " + maxUnackedRangesToPersist + ", Text-size = "
                + contentText.length + ", " + "Binary-size= " + contentBinary.length + ", size-multiplier= "
                + (contentText.length / contentBinary.length));

    }

    private Iterable<? extends MessageRange> buildIndividualDeletedMessageRanges(int maxUnackedRangesToPersist) {

        final RangeSet<PositionImpl> individualDeletedMessages = TreeRangeSet.create();
        int rangeLimit = 0;
        for (int i = 0; i < maxUnackedRangesToPersist; i++) {
            Range<PositionImpl> range = Range.openClosed(new PositionImpl(0, rangeLimit++),
                    new PositionImpl(0, rangeLimit++));
            rangeLimit++;
            individualDeletedMessages.add(range);
        }
        MLDataFormats.NestedPositionInfo.Builder nestedPositionBuilder = MLDataFormats.NestedPositionInfo.newBuilder();
        MLDataFormats.MessageRange.Builder messageRangeBuilder = MLDataFormats.MessageRange.newBuilder();
        return individualDeletedMessages.asRanges().stream().limit(maxUnackedRangesToPersist).map(positionRange -> {
            PositionImpl p = positionRange.lowerEndpoint();
            nestedPositionBuilder.setLedgerId(p.getLedgerId());
            nestedPositionBuilder.setEntryId(p.getEntryId());
            messageRangeBuilder.setLowerEndpoint(nestedPositionBuilder.build());
            p = positionRange.upperEndpoint();
            nestedPositionBuilder.setLedgerId(p.getLedgerId());
            nestedPositionBuilder.setEntryId(p.getEntryId());
            messageRangeBuilder.setUpperEndpoint(nestedPositionBuilder.build());
            return messageRangeBuilder.build();
        }).collect(Collectors.toList());
    }
}
