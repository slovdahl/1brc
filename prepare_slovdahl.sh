#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# Uncomment below to use sdk
source "$HOME/.sdkman/bin/sdkman-init.sh"

if [ "$1" = "graal" ]; then
    sdk use java 21.0.2-graal 1>&2 > /dev/null
    NATIVE_IMAGE_OPTS="-dsa -O3 -march=native -H:-GenLoopSafepoints --enable-preview --initialize-at-build-time=dev.morling.onebrc.CalculateAverage_slovdahl"
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_slovdahl_image dev.morling.onebrc.CalculateAverage_slovdahl
elif [ "$1" = "jdk" ]; then
    rm -f target/CalculateAverage_slovdahl_image
    sdk use java 21.0.2-tem 1>&2 > /dev/null
    ./mvnw verify
else
    echo "Specify either graal or jdk"
    exit 1
fi
