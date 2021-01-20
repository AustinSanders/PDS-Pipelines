#!/usr/bin/env python
import sys
import logging
import argparse
import json
import pathlib
import glob
from shutil import copy2, disk_usage
from os.path import getsize, dirname, splitext, exists, basename, join
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_log, pds_db, workarea, disk_usage_ratio, archive_base
from pds_pipelines.queueing import parse_args, DerivedQueueProcess

def main(user_args):

    archive = user_args.archive
    volume = user_args.volume
    search = user_args.search
    log_level = user_args.log_level

    process = DerivedQueueProcess('Derived', archive, volume, search, log_level)
    matching_files = process.get_matching_files()
    process.run(matching_files)


if __name__ == "__main__":
    sys.exit(main(parse_args()))
