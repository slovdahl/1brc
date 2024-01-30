/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;

public class CalculateAverage_slovdahl {

    private static final String FILE = "./measurements.txt";

    private static final long NEWLINE_PATTERN = compilePattern((byte) '\n');
    private static final long SEMICOLON_PATTERN = compilePattern((byte) ';');
    private static final VarHandle TO_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int segments = Runtime.getRuntime().availableProcessors() - 1;

        try (Arena arena = Arena.ofShared();
                FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
                ExecutorService executor = Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory())) {

            long size = channel.size();
            long idealSegmentSize = size / segments;
            MemorySegment mappedFile = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            var futures = new ArrayList<Future<Map<Station, MeasurementAggregator>>>(segments);

            long segmentStart = 0;
            for (int i = 1; i <= segments; i++) {
                long actualSegmentOffset = idealSegmentSize * i;

                while (actualSegmentOffset < size && mappedFile.get(ValueLayout.JAVA_BYTE, actualSegmentOffset) != (byte) '\n') {
                    actualSegmentOffset++;
                }

                long end = actualSegmentOffset - segmentStart;
                if (segmentStart + actualSegmentOffset - segmentStart + 1 < size) {
                    end += 1;
                }

                MemorySegment segment = mappedFile.asSlice(segmentStart, end);
                segmentStart = actualSegmentOffset + 1;

                futures.add(executor.submit(() -> {
                    int sliceSize = 1048576;
                    byte[] array = new byte[sliceSize];
                    MemorySegment bufferSegment = MemorySegment.ofArray(array);

                    long position = 0;
                    long segmentSize = segment.byteSize();
                    Map<Station, MeasurementAggregator> map = HashMap.newHashMap(10_000);

                    while (position < segmentSize) {
                        long thisSliceSize = Math.min(sliceSize, segmentSize - position);

                        MemorySegment.copy(
                                segment,
                                ValueLayout.JAVA_BYTE,
                                position,
                                bufferSegment,
                                ValueLayout.JAVA_BYTE,
                                0,
                                thisSliceSize);

                        if (thisSliceSize % 8 != 0) {
                            bufferSegment
                                    .asSlice(thisSliceSize)
                                    .fill((byte) 0);
                        }

                        int newlinePosition = 0;
                        int startOffset = 0;
                        int swarOffset = 0;
                        while (true) {
                            int semicolonPosition = swar(array, SEMICOLON_PATTERN, swarOffset);
                            if (semicolonPosition < 0) {
                                break;
                            }

                            swarOffset = (semicolonPosition / Long.BYTES) * Long.BYTES;

                            int eolPosition = swar(array, NEWLINE_PATTERN, swarOffset);
                            if (eolPosition < 0) {
                                break;
                            }
                            newlinePosition = eolPosition;

                            byte[] nameArray = new byte[semicolonPosition - startOffset];
                            System.arraycopy(array, startOffset, nameArray, 0, semicolonPosition - startOffset);
                            Station station = new Station(nameArray);

                            int temperatureStart = semicolonPosition + 1;
                            int temperatureLength = newlinePosition - semicolonPosition - 1;

                            int intValue;
                            if (array[temperatureStart] == '-') {
                                if (temperatureLength == 4) {
                                    intValue = -(array[temperatureStart + 1] - 48) * 10 +
                                            (array[temperatureStart + 3] - 48);
                                }
                                else {
                                    intValue = -(array[temperatureStart + 1] - 48) * 100 +
                                            (array[temperatureStart + 2] - 48) * 10 +
                                            (array[temperatureStart + 4] - 48);
                                }
                            }
                            else {
                                if (temperatureLength == 3) {
                                    intValue = (array[temperatureStart] - 48) * 10 +
                                            (array[temperatureStart + 2] - 48);
                                }
                                else {
                                    intValue = (array[temperatureStart] - 48) * 100 +
                                            (array[temperatureStart + 1] - 48) * 10 +
                                            (array[temperatureStart + 3] - 48);
                                }
                            }

                            MeasurementAggregator agg = map.get(station);
                            if (agg == null) {
                                agg = new MeasurementAggregator();
                                map.put(station, agg);
                            }

                            agg.min = Math.min(agg.min, intValue);
                            agg.max = Math.max(agg.max, intValue);
                            agg.sum += intValue;
                            agg.count++;

                            // Make sure the next iteration won't find the same delimiters.
                            array[semicolonPosition] = (byte) 0;
                            array[newlinePosition] = (byte) 0;

                            startOffset = newlinePosition + 1;
                            swarOffset = (newlinePosition / Long.BYTES) * Long.BYTES;
                        }

                        position += newlinePosition + 1;
                    }

                    return map;
                }));
            }

            TreeMap<String, ResultRow> result = futures.stream()
                    .map(f -> {
                        try {
                            return f.get();
                        }
                        catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .flatMap(m -> m.entrySet().stream())
                    .collect(groupingBy(
                            e -> new String(e.getKey().name()),
                            TreeMap::new,
                            collectingAndThen(
                                    reducing(
                                            new MeasurementAggregator(),
                                            Map.Entry::getValue,
                                            (agg1, agg2) -> {
                                                MeasurementAggregator res = new MeasurementAggregator();
                                                res.min = Math.min(agg1.min, agg2.min);
                                                res.max = Math.max(agg1.max, agg2.max);
                                                res.sum = agg1.sum + agg2.sum;
                                                res.count = agg1.count + agg2.count;

                                                return res;
                                            }),
                                    agg -> new ResultRow(
                                            agg.min / 10.0,
                                            (Math.round((agg.sum / 10.0) * 10.0) / 10.0) / agg.count,
                                            agg.max / 10.0))));

            System.out.println(result);

            executor.shutdownNow();
        }
    }

    private static long compilePattern(byte byteToFind) {
        long pattern = byteToFind & 0xFFL;
        return pattern
                | (pattern << 8)
                | (pattern << 16)
                | (pattern << 24)
                | (pattern << 32)
                | (pattern << 40)
                | (pattern << 48)
                | (pattern << 56);
    }

    private static int swar(byte[] data, long pattern, int offset) {
        while (offset < data.length) {
            int index = firstInstance(getWord(data, offset), pattern);
            offset += Long.BYTES;
            if (index < Long.BYTES) {
                return offset - index - 1;
            }
        }

        return -1;
    }

    private static int firstInstance(long word, long pattern) {
        long input = word ^ pattern;
        long tmp = (input & 0x7F7F7F7F7F7F7F7FL) + 0x7F7F7F7F7F7F7F7FL;
        tmp = ~(tmp | input | 0x7F7F7F7F7F7F7F7FL);
        return Long.numberOfLeadingZeros(tmp) >>> 3;
    }

    private static long getWord(byte[] data, int index) {
        return (long) TO_LONG.get(data, index);
    }

    private record Station(byte[] name, int hash) {
        private Station(byte[] name) {
            this(name, Arrays.hashCode(name));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Station station = (Station) o;
            return Arrays.equals(name, station.name);
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    private static class MeasurementAggregator {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private long count;
    }

    private record ResultRow(double min, double mean, double max) {

        @Override
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }
}
