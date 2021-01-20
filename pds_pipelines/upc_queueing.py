#!/usr/bin/env python

import sys
import logging
import argparse
import json
import pathlib
import glob
from os.path import getsize, dirname, splitext
from shutil import copy2, disk_usage
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.config import pds_log, pds_info, pds_db, workarea, disk_usage_ratio
from pds_pipelines.queueing import parse_args, UPCQueueProcess

def main(user_args):

    archive = user_args.archive
    volume = user_args.volume
    search = user_args.search
    log_level = user_args.log_level
    namespace = user_args.namespace

    process = UPCQueueProcess('UPC', archive, volume, search, log_level, namespace)
    matching_files = process.get_matching_files()
    process.run(matching_files)


if __name__ == "__main__":
    sys.exit(main(parse_args()))
