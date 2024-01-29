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
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.stream.Collectors.*;

public class CalculateAverage_slovdahl {

    private static final String FILE = "./measurements.txt";

    private static final long NEWLINE_PATTERN = compilePattern((byte) '\n');
    private static final long SEMICOLON_PATTERN = compilePattern((byte) ';');
    private static final VarHandle TO_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        int segments = Runtime.getRuntime().availableProcessors() / 2;
        System.out.println("Segments: " + segments);

        try (Arena arena = Arena.ofShared();
                FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
                ExecutorService executor = Executors.newThreadPerTaskExecutor(Executors.defaultThreadFactory())) {

            long size = channel.size();
            long idealSegmentSize = size / segments;
            MemorySegment mappedFile = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>(segments);

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
                    Map<String, MeasurementAggregator> measurementAggregator = new HashMap<>();

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
                            int eolPosition = swar(array, NEWLINE_PATTERN, swarOffset);
                            if (eolPosition < 0) {
                                break;
                            }
                            newlinePosition = eolPosition;

                            int semicolonPosition = swar(array, SEMICOLON_PATTERN, swarOffset);
                            if (semicolonPosition < 0) {
                                throw new IllegalStateException();
                            }

                            byte[] temperatureArray = new byte[newlinePosition - semicolonPosition - 1];
                            System.arraycopy(array, semicolonPosition + 1, temperatureArray, 0, newlinePosition - semicolonPosition - 1);

                            String station = new String(array, startOffset, semicolonPosition - startOffset);

                            // TODO: parse as int and divide with 10
                            double value = Double.parseDouble(new String(temperatureArray));

                            MeasurementAggregator agg = measurementAggregator.computeIfAbsent(station, s -> new MeasurementAggregator());

                            agg.min = Math.min(agg.min, value);
                            agg.max = Math.max(agg.max, value);
                            agg.sum += value;
                            agg.count++;

                            array[semicolonPosition] = (byte) 0;
                            array[newlinePosition] = (byte) 0;

                            startOffset = newlinePosition + 1;
                            swarOffset = (newlinePosition / Long.BYTES) * Long.BYTES;
                        }

                        position += newlinePosition + 1;
                    }

                    return measurementAggregator;
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
                            Map.Entry::getKey,
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
                                    agg -> new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max))));

            System.out.println(result);

            executor.shutdownNow();
        }
    }

    private record Measurement(String station, double value) {
        static Measurement of(String[] parts) {
            byte[] measurementValue = parts[1].getBytes(StandardCharsets.US_ASCII);

            int value;
            if (measurementValue[0] == '-') {
                if (measurementValue.length == 4) {
                    value = -Character.getNumericValue(measurementValue[1]) * 10 +
                            Character.getNumericValue(measurementValue[3]);
                } else {
                    value = -Character.getNumericValue(measurementValue[1]) * 100 +
                            Character.getNumericValue(measurementValue[2]) * 10 +
                            Character.getNumericValue(measurementValue[4]);
                }
            } else {
                if (measurementValue.length == 3) {
                    value = Character.getNumericValue(measurementValue[0]) * 10 +
                            Character.getNumericValue(measurementValue[2]);
                } else {
                    value = Character.getNumericValue(measurementValue[0]) * 100 +
                            Character.getNumericValue(measurementValue[1]) * 10 +
                            Character.getNumericValue(measurementValue[3]);
                }
            }

            return new Measurement(parts[0], value);
        }
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

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
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
}
