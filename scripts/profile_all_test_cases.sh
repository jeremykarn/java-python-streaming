#!/bin/bash

scripts/python_streaming_profiler.sh data/number_data.txt
scripts/python_streaming_profiler.sh data/string_data.txt
scripts/python_streaming_profiler.sh data/long_string_data.txt
scripts/python_streaming_profiler.sh data/shallow_map_data.txt
scripts/python_streaming_profiler.sh data/deep_map_data.txt
scripts/python_streaming_profiler.sh data/embedded_tuple_data.txt
scripts/python_streaming_profiler.sh data/bag_data.txt
scripts/python_streaming_profiler.sh data/bag_of_short_strings_data.txt

scripts/print_profile.sh output/python_streaming_number_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_string_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_long_string_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_shallow_map_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_deep_map_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_embedded_tuple_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_bag_data.profile | head -n 25 >>$1
echo "" >>$1
scripts/print_profile.sh output/python_streaming_bag_of_short_strings_data.profile | head -n 25 >>$1
echo "" >>$1
