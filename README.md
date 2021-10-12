# PDS-Pipelines
A combination of PDS software for data integrity, universal planetary coordinates, ingestion, services (POW/MAP2), etc.

[![codecov](https://codecov.io/gh/USGS-Astrogeology/PDS-Pipelines/branch/master/graph/badge.svg?token=APOR01UYWU)](https://codecov.io/gh/USGS-Astrogeology/PDS-Pipelines)


## Using PDS-Pipelines Locally

PDS-Pipelines is not currently available via anaconda or other distribution
platforms. PDS-Pipelines also requires anaconda, and docker. This use case is
largely developer centric and will get technical with the tools used to run
PDS-Pipelines

First cd into the cloned repository, and run:

    conda env create -f environment.yml

This will create a fresh environment called `PDS-Pipelines`. Then activate said
environment with:

    conda activate PDS-Pipelines

You'll then want to actually install PDS-Pipelines with the following:

    python setup.py develop

You will want to modify the `config.py` file within `pds_pipelines` and change the
`scratch` variable to some path. Usually, this is just a path to the repo but
you can make your "scratch" area anywhere on your machine. In this path, we'll
call it "Path/to/scratch", you have to make the `workarea` folder.

First, you will want to replace the scratch variable in the `config.py` file
within `pds_pipelines` with the "/Path/to/scratch":

    scratch = /Path/to/scratch/

Then create the workarea folder under the "/Path/to/scratch" directory:

    mkdir /Path/to/scratch/workarea

Next, we'll have to create the `logs`, and `output` folders. These folders usually
exist at the root level of the PDS-Pipelines repository.  Along with this, you
will need to modify the `root` variable in the `config.py` file within `pds_pipelines`.
We will change it to the full path to the cloned PDS-Pipelines repo. In other
words we should set the following within `config.py`:

    root = /Path/from/root/to/repo/PDS-Pipelines

Followed by:

    mkdir /Path/from/root/to/repo/PDS-Pipelines/logs
    mkdir /Path/from/root/to/repo/PDS-Pipelines/output

Then, run the following to setup the docker containers:

    cd containers
    export /Path/to/the/database docker-compose up

The above path should be a fresh, empty folder in a fairly large portion of some drive
on your computer. This folder will contain the on disk storage for the database
and allow you to load a previous database if one exists there already.

From here you can do one of two things, start processing data, or manually create
the databases that UPC, MAP and POW2 depend on. It's recommended that you start
by manually creating the databases as there are a few other idiosyncrasies that
crop up while running locally.

First run the following in a python instance with the above conda env
activated:

    from pds_pipelines.models import upc_models, pds_models
    upc_models.create_upc_database()
    pds_models.create_pds_database()

This creates the necessary databases for both the DI database and the UPC database,
from here things get a little more complicated. Within the PDS-Pipelines repo
there is a file named `PDSinfo.json`. This file maps various missions/instruments
to known archive within the DI database. Here is an example record within the json
file:

    "mro_ctx":
    {
        "archiveid": "16",
        "path": "/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX/",
        "mission": "CTX",
        "bandbinQuery" : "FilterName",
        "upc_reqs": ["/data/", ".IMG"]
    }

As you can see, you probably don't have the above path on your computer. To gain
access to these files you will need to mirror the PDS_Archive that is maintained
by various entities (such as the USGS). Some images that the PDS_Archive maintains
can be found [here](https://pds-imaging.jpl.nasa.gov/volumes/). From here you will
need to extract files from there respective missions/instruments into the path
defined in the PDSinfo.json. You can do this one of two ways, either mirror the
paths defined in the PDSinfo.json for the mission you are working with, OR you
can update the path in the PDSinfo.json to point to where you have downloaded
files. The latter is much easier but will likely not be able to take advantage
of features supported by the pipelines.

Example (Using CTX and the above PDS nodes):

Navigate to the following:

    https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/

Pull an image from the archive, lets pick `P20_008794_2573_XN_77N268W.IMG`. The
first image in the volume.

Now we make the above file structure:

    mkdir -p /pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/

Ideally, working at the root level should only be done on a personal system.

Now we move the file we have pulled to this location:

    mv /Your/Downloads/Folder/P20_008794_2573_XN_77N268W.IMG /pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/

    or

    wget -O /pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/P20_008794_2573_XN_77N268W.IMG https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/P20_008794_2573_XN_77N268W.IMG

Alternative (Using CTX and the above PDS nodes):

    Either download the files to your downloads and move it to a new directory
    or wget it directly to your new directory like:

    wget -O /Path/to/CTX/area/mrox_0602/data/P20_008794_2573_XN_77N268W.IMG https://pdsimage2.wr.usgs.gov/Missions/Mars_Reconnaissance_Orbiter/CTX/mrox_0602/data/P20_008794_2573_XN_77N268W.IMG

Then update the `PDSinfo.json` record to the following:

    "mro_ctx":
    {
        "archiveid": "16",
        "path": "/Path/to/CTX/area/",
        "mission": "CTX",
        "bandbinQuery" : "FilterName",
        "upc_reqs": ["/data/", ".IMG"]
    }

In both cases we want to keep the "volume" (mrox_0602) and data directories due
to `upc_reqs` in the `PDSinfo.json` entry. The `upc_reqs` define what strings need to be
present in a file path for that file to be considered for ingestion into the DI database.
For MRO_CTX these requirements ensure that only files that contain a "data" directory and contain
".IMG" are ingested into the DI database. As such, to get our file into the DI database
we can either retain the file structure, or update the `upc_reqs` by removing the
"/data/" from the requirements. For the sake of simplicity, we are going to retain
the file structure and try to minimize how much be change the `PDSinfo.json` file.


Ideally, if you have done either of the above two options, you will now be able
run both `ingest_queueing.py` then `ingest_proces.py`. First you'll need to cd to
the root of the repository:

    cd /Path/to/repo/PDS-Pipelines

Then you should be able to run the following:

    python pds_pipelines/ingest_queueing.py -a mro_ctx
    python pds_pipelines/ingest_process.py

Now, lets interrogate the database to see if our file is there:

    psql -h localhost -p 5432 di_test postgres

This will connect us to our local DI database, then we run the following to see
what records are now documented within the DI database:

    select * from files;

There should be one record in the table with `mrox_0602/data/P20_008794_2573_XN_77N268W.IMG`
in the `filename` field and the `upc_required` field should be set to `t` for true.
This means the file is recognized as a UPC eligible file that can be processed
into the UPC database.

From here you will need to install ISIS as processing for UPC heavily relies on
ISIS to generate the necessary data for the UPC database. Follow the
tutorials presented by the ISIS team [here](https://github.com/USGS-Astrogeology/ISIS3/blob/dev/README.md#Installation).
As a recommendation, install ISIS into a different conda environment,
then point PDS-Pipelines at that conda environment.

We assume that you have ISIS installed correctly and have set ISISROOT to the
conda environment created above. You will also need the ISIS data area which
can be obtained from [here](https://github.com/USGS-Astrogeology/ISIS3/blob/dev/README.md#The-ISIS-Data-Area).
Then set ISIS_DATA to where the data was pulled into.

Now, you should be able to run the following to generate an entry into the UPC
database:

    python pds_pipelines/upc_queueing.py -a mro_ctx
    python pds_pipelines/upc_process.py

This may take some time as the image we are working with is quite large. Once this
is finished, we can interrogate the database similarly to how we examined the di_test
database:

    psql -h localhost -p 5432 upc_test postgres

Again, this connects us to the associated database. Then we run:

    select * from datafiles;

There should be one record whose `source` field contains
`mrox_0602/data/P20_008794_2573_XN_77N268W.IMG`. If that is true, then success!
You have ingested a record from start to finish into the local UPC database
