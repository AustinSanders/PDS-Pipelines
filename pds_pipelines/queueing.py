import argparse
import json
import logging
import pathlib
import glob

from shutil import copy2, disk_usage
from sqlalchemy import Date, cast
from sqlalchemy import or_
from os.path import getsize, dirname, splitext, exists, basename, join

from pds_pipelines.models.pds_models import Files
from pds_pipelines.db import db_connect
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.config import pds_info, pds_log, pds_db, workarea, disk_usage_ratio, archive_base

def parse_args():
    parser = argparse.ArgumentParser(description='UPC Queueing')

    parser.add_argument('--archive', '-a', dest="archive", required=True,
                        help="Enter archive - archive to ingest")

    parser.add_argument('--volume', '-v', dest="volume",
                        help="Enter volume to Ingest")

    parser.add_argument('--search', '-s', dest="search",
                        help="Enter string to search for")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO',
                                'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    parser.add_argument('--namespace', '-n', dest="namespace",
                        help="The namespace used for this queue.")

    args = parser.parse_args()
    return args



def copy_files(fname, src_dir, dest_dir):
    """ Globs files with shared filename in src_dir and copies them to dest_dir

    Parameters
    ----------
    fname : str
        The name of the file

    src_dir : str
        The directory from which to copy the files

    dest_dir : str
        The directory to which to copy the files

    Returns
    -------
    str
        The destination of the file

    """
    dest_path = dirname(fname)
    dest_path = dest_path.replace(src_dir, dest_dir)
    pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)
    for f in glob.glob(splitext(fname)[0] + r'.*'):
        copy2(f, dest_path)
    return join(dest_path, fname)


def has_space(elements, src_path, dest_path, ratio):
    if elements:
        addcount = 0
        size = 0
        for element in elements:
            fname = src_path + element.filename
            size += getsize(fname)

        size_free = disk_usage(dest_path).free
        if size >= (disk_usage_ratio * size_free ):
            return False
    return True


class QueueProcess():
    def __init__(self, process_name, archive, volume=None, search=None, log_level='INFO', namespace=None):
        self.process_name = process_name
        self.archive = archive
        self.logger = self.get_logger(log_level)
        self.archive_info = json.load(open(pds_info, 'r'))
        try:
            self.archive_id = self.get_archive_id()
        except KeyError:
            self.logger.error("Archive %s not found in %s", archive, pds_info)
            exit()
        self.volume = volume
        self.search = search
        self.process_queue = RedisQueue(f"{process_name}_ReadyQueue", namespace)
        self.error_queue = RedisQueue(f"{process_name}_ErrorQueue", namespace)
        self.logger.info("%s queue: %s", process_name, self.process_queue.id_name)

        try:
            pds_session_maker, _ = db_connect(pds_db)
            self.logger.info('Database Connection Success')
        except Exception as e:
            self.logger.error('Database Connection Error\n\n%s', e)
            exit()

        self.session_maker = pds_session_maker


    def get_logger(self, log_level):
        logger = logging.getLogger(f"{self.process_name}_Queueing.{self.archive}")
        level = logging.getLevelName(log_level)
        logger.setLevel(level)
        logFileHandle = logging.FileHandler(pds_log + 'Process.log')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
        logFileHandle.setFormatter(formatter)
        logger.addHandler(logFileHandle)
        return logger


    def get_matching_files(self):
        raise NotImplementedError()


    def enqueue(self, element):
        raise NotImplementedError()


    def run(self, elements=None, copy=True):
        """ Copies and queues a set of elements into the process's queue(s)

        Parameters
        ----------
        elements : sqlalchemy.orm.query.Query
            The elements to be processed

        copy : boolean
            If true, copies files to the work area.  If false, skips copying step.

        Returns
        -------
        None
        """
        source_path = self.archive_info[self.archive]['path']
        if copy and not has_space(elements, source_path, workarea, disk_usage_ratio):
            self.logger.error("Unable to copy files: Insufficient disk space in %s.", workarea)
            raise IOError(f"Insufficient disk space in {workarea}.")

        addcount=0
        for element in elements:
            try:
                self.enqueue(element, copy)
                addcount = addcount + 1
            except Exception as e:
                self.error_queue.QueueAdd(f'Unable to copy / queue {element.filename}: {e}')
                self.logger.error('Unable to copy / queue %s: %s', element.filename, e)
        self.logger.info('Files Added to %s Queue: %s', self.process_name, addcount)


    def get_archive_id(self):
        """ Get the archive id from this process's archive name

        Returns
        -------

        archive_id : int
            The integer value associated with the archive name.

        """
        try:
            archive_id = self.archive_info[self.archive]['archiveid']
        except KeyError:
            print("\nArchive '{}' not found in {}\n".format(archive, info))
            print("The following archives are available:")
            for k in info.keys():
                print("\t{}".format(k))
            raise

        return archive_id


class DIQueueProcess(QueueProcess):
    def get_matching_files(self):
        session = self.session_maker()
        td = (datetime.datetime.now(pytz.utc) -
              datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")
        if self.volume:
            volstr = '%' + self.volume + '%'
            results = session.query(Files).filter(
                Files.archiveid == archiveID, Files.filename.like(volstr)).filter(
                    or_(cast(Files.di_date, Date) < testing_date,
                        cast(Files.di_date, Date) is None))
        else:
            results = session.query(Files).filter(Files.archiveid == archiveID).filter(
                or_(cast(Files.di_date, Date) < testing_date,
                    cast(Files.di_date, Date) is None))
        sessin.close()

        return results

    def enqueue(self, element, copy):
        self.process_queue.add((element.filename, self.archive))



class UPCQueueProcess(QueueProcess):
    def get_matching_files(self):
        session = self.session_maker()
        if self.volume:
            volstr = '%' + self.volume + '%'
            results = session.query(Files).filter(Files.archiveid == self.archive_id,
                                               Files.filename.like(volstr),
                                               Files.upc_required == 't')
        else:
            results = session.query(Files).filter(Files.archiveid == self.archive_id,
                                               Files.upc_required == 't')
        if self.search:
            qf = '%' + self.search + '%'
            results = results.filter(Files.filename.like(qf))
        session.close()
        return results

    def enqueue(self, element, copy):
        path = self.archive_info[self.archive]['path']
        fname = path+element.filename
        if copy:
            fname = copy_files(path+fname, archive_base, workarea)
        self.process_queue.QueueAdd((workarea+element.filename, element.fileid, self.archive))

class DerivedQueueProcess(UPCQueueProcess):
    def enqueue(self, element, copy):
        path = self.archive_info[self.archive]['path']
        fname = path+element.filename
        if copy:
            fname = copy_files(path+fname, archive_base, workarea)
        self.process_queue.QueueAdd((fname, self.archive))
