from pds_pipelines.db import db_connect
from pds_pipelines.notify import setup_smtp, notify_finished, notify_error


def main():
    session, _ = db_connect('clusterjob_prd')
    server = setup_smtp()
    notify_finished(server, session)
    notify_error(server, session)


if __name__ == "__main__":
    main()
