#!/usr/bin/env python
import argparse
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
