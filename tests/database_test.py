from shapely.geometry import Polygon
from pds_pipelines.db import db_connect
from pds_pipelines.models import upc_models
from pds_pipelines.config import upc_db
import numpy as np
import datetime
import pytz

def main():
    session, engine = db_connect(upc_db)
    size = int(1e7) - int(1e6)
    chunk = int(1e6)

    for j in range(size//chunk):

        lon = np.random.uniform(-179, 179, chunk)
        lat = np.random.uniform(-89, 89, chunk)
        sol_lon = np.random.uniform(0, 360, chunk)
        mgr = np.random.uniform(4, 6, chunk)
        emission_angle = np.random.uniform(2, 10, chunk)
        incidence_angle = np.random.uniform(40, 80, chunk)
        phase_angle = np.random.uniform(40, 80, chunk)

        db_input = []

        for i in range(chunk):
            ul = (lon[i], lat[i])
            ll = (lon[i], lat[i] - 0.5)
            lr = (lon[i] - 0.5, lat[i] - 0.5)
            ur = (lon[i] - 0.5, lat[i])

            p = Polygon([ul, ll, lr, ur, ul])

            upc_time = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            db_input.append(upc_models.SearchTerms(upctime=upc_time,
                                            starttime=upc_time,
                                            solarlongitude=sol_lon[i],
                                            meangroundresolution=mgr[i],
                                            minimumemission=emission_angle[i]-2,
                                            maximumemission=emission_angle[i]+2,
                                            emissionangle=emission_angle[i],
                                            minimumincidence=incidence_angle[i]-2,
                                            maximumincidence=incidence_angle[i]+2,
                                            incidenceangle=incidence_angle[i],
                                            minimumphase=phase_angle[i]-2,
                                            maximumphase=phase_angle[i]+2,
                                            phaseangle=phase_angle[i],
                                            targetid='mars',
                                            instrumentid='CTX',
                                            missionid=None,
                                            pdsproductid=None,
                                            err_flag=False,
                                            isisfootprint=p.wkt))
        session.bulk_save_objects(db_input)
        session.commit()

                    
        

if __name__ == '__main__':
    main()
