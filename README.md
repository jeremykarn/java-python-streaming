To profile Python streaming (can substitute number\_data with any of the test datasets created):

    ./compilejava.sh
    ./gen_test_data.sh
    ./python_streaming_profiler.sh data/number_data.txt
    ./print_profile output/python_streaming_number_data.profile

Remember to put the proper pig-withouthadoop.jar into lib depending on if you're profiling old code or new code.