from pds_pipelines.notify import setup_smtp, notify_upcoming_purge
from pds_pipelines.db import db_connect

def main():
    Session, _ = db_connect('clusterjob_prd')
    session = Session()
    server = setup_smtp()
    notify_upcoming_purge(server, session)


if __name__ == "__main__":
    main()
