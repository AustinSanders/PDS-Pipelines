#!/usr/bin/env python

import sys
import datetime
import logging
import argparse
import json
import pytz

from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_db, pds_log
from pds_pipelines.queueing import parse_args, DIQueueProcess
from sqlalchemy import Date, cast
from sqlalchemy import or_



def main(user_args):
    archive = user_args.archive
    volume = user_args.volume
    jobarray = user_args.jobarray
    log_level = user_args.log_level
    namespace = user_args.namespace
    process = DIQueueProcess('DI', archive, volume, search, log_level, namespace)
    matching_files = process.get_matching_files()
    process.run(matching_files, copy=False)

if __name__ == "__main__":
    sys.exit(main(parse_args()))
