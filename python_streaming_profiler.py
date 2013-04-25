#!/usr/bin/python

import cProfile
import sys

from controller import PythonStreamingController

# also have to pipe input data file through stdin
input_data_path = sys.argv[1]
input_data_stem = input_data_path.split('/')[-1].rsplit('.', 1)[0]

out = "output/python_streaming_%s.out" % (input_data_stem)
err = "output/python_streaming_%s.err" % (input_data_stem)
log = "output/python_streaming_%s.log" % (input_data_stem)
profile_out = "output/python_streaming_%s.profile" % (input_data_stem)

open(out, 'w').close()
open(err, 'w').close()
open(log, 'w').close()

pr = cProfile.Profile()
pr.enable()

controller = PythonStreamingController()
controller.main(module_name="fake_udfs",
                file_path="fake_udfs.py",
                func_name="wrap_args",
                cache_path="/dev/null",
                output_stream_path=out,
                error_stream_path=err,
                log_file_name=log,
                is_illustrate_str="false")

pr.disable()
pr.dump_stats(profile_out)
