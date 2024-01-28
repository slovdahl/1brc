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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CalculateAverage_slovdahl {

    private static final String FILE = "./measurements.txt";

    private static final long NEWLINE_PATTERN = compilePattern((byte) '\n');
    private static final long SEMICOLON_PATTERN = compilePattern((byte) ';');
    private static final VarHandle TO_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
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

    public static void main(String[] args) throws IOException {
        // [station name max length 100];[temp max length 5] = 106 max per line

        List<Measurement> measurements;
        if (args[0].equals("ffm")) {
            measurements = new ArrayList<>();
            try (Arena arena = Arena.ofConfined();
                    FileChannel channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {

                long size = channel.size();
                // int sliceSize = 8;
                // int sliceSize = 112; // 22-23 s
                int sliceSize = 1024;
                // int sliceSize = 96;
                // int sliceSize = 224;
                long position = 0;
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);

                while (position < size) {
                    long thisSliceSize = Math.min(sliceSize, size - position);
                    byte[] array = segment.asSlice(position, thisSliceSize).toArray(ValueLayout.JAVA_BYTE);

                    if (array.length % 8 != 0) {
                        byte[] paddedArray = new byte[((array.length / 8) + 1) * 8];
                        System.arraycopy(array, 0, paddedArray, 0, array.length);
                        array = paddedArray;
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

                        Measurement m = new Measurement(
                                new String(array, startOffset, semicolonPosition - startOffset),
                                // TODO: parse and store as int
                                Double.parseDouble(new String(temperatureArray)));

                        measurements.add(m);

                        array[semicolonPosition] = (byte) 0;
                        array[newlinePosition] = (byte) 0;

                        startOffset = newlinePosition + 1;
                        swarOffset = (newlinePosition / Long.BYTES) * Long.BYTES;
                    }

                    position += newlinePosition + 1;
                }
            }
        }
        else {
            measurements = Files.lines(Paths.get(FILE))
                    .map(l -> {
                        String[] split = l.split(";");
                        return new Measurement(split[0], Double.parseDouble(split[1]));
                    })
                    .toList();
        }

        System.out.println("Size: " + measurements.size());

        /*
         * Map<String, ResultRow> unsortedResults = Files.lines(Paths.get(FILE))
         * .map(l -> {
         * int position = swar(l.getBytes(StandardCharsets.US_ASCII), SEMICOLON_PATTERN);
         * String station = l.substring(0, position);
         * String valueString = l.substring(position + 1);
         * double value = Double.parseDouble(valueString);
         * return new Measurement(station, value);
         * })
         * .collect(
         * groupingBy(
         * Measurement::station,
         * Collector.of(
         * MeasurementAggregator::new,
         * (a, m) -> {
         * a.min = Math.min(a.min, m.value);
         * a.max = Math.max(a.max, m.value);
         * a.sum += m.value;
         * a.count++;
         * },
         * (agg1, agg2) -> {
         * var res = new MeasurementAggregator();
         * res.min = Math.min(agg1.min, agg2.min);
         * res.max = Math.max(agg1.max, agg2.max);
         * res.sum = agg1.sum + agg2.sum;
         * res.count = agg1.count + agg2.count;
         *
         * return res;
         * },
         * agg -> new ResultRow(agg.min, (Math.round(agg.sum * 10.0) / 10.0) / agg.count, agg.max)
         * )
         * )
         * );
         *
         * Map<String, ResultRow> measurements = new TreeMap<>(unsortedResults);
         *
         * System.out.println(measurements);
         */
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
