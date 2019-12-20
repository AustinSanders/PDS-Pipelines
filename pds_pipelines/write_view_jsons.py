from pds_pipelines.db import db_connect
from pds_pipelines.config import upc_db, view_path
import os
import json

queries = {
    'volume_summary': """
        SELECT d.instrumentid, d.targetid, t.targetname, t.system, i.instrument, i.mission, i.spacecraft, i.displayname,
        count(d.*) as image_count, min(s.starttime) as start_date, max(s.starttime) as stop_date, max(s.processdate) as publish_date
        FROM datafiles d
        JOIN instruments i on (i.instrumentid=d.instrumentid)
        JOIN targets t on (t.targetid=d.targetid)
        JOIN search_terms s on (d.upcid=s.upcid)
        GROUP by d.instrumentid, i.instrument, i.mission, i.spacecraft, d.targetid, t.targetname, t.system, i.displayname
        """,
    'band_summary': """
        SELECT DISTINCT i.instrumentid, i.instrument, j.jsonkeywords -> 'caminfo' -> 'isislabel' -> 'isiscube' -> 'bandbin' ->> 'filtername' AS filtername, 
        j.jsonkeywords -> 'caminfo' -> 'isislabel' -> 'isiscube' -> 'bandbin' -> 'center' ->> 0 AS center
        FROM instruments i
        JOIN datafiles d on (d.instrumentid = i.instrumentid)
        JOIN json_keywords j on (d.upcid = j.upcid)
        """
}

def parse_args():
    parser = argparse.ArgumentParser(description='Create view JSONs.')

    parser.add_argument('--path', '-p', dest="path", required=False,
                        help="Enter path - where to write the JSONs.")
    
    args = parser.parse_args()
    return args


def main(user_args):
    if user_args.path:
        path = user_args.path
    else:
        path = view_path

    session, _ = db_connect(upc_db)
    
    for key in queries:
        json_query = "with t AS ({}) SELECT json_agg(t) FROM t;".format(queries[key])
        output = session.execute(json_query)
        json_output = json.dumps([dict(line) for line in output])

        with open(path + key + ".json", "w") as json_file:
            json_file.write(json_output)

if __name__ == "__main__":
    main(parse_args())
