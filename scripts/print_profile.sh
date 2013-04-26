#!/usr/bin/python

import pstats
import sys

ps = pstats.Stats(sys.argv[1])
ps.sort_stats(1)
ps.print_stats()
