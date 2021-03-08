import warnings
from os import rename
available_modules = {}

try:
    from pysis import isis
    available_modules['isis'] = isis
except Exception as e:
    warnings.warn('Unable to add isis to the available modules. ' +
                 f'Failed with the following {e}.')

try:
    from osgeo import gdal
    available_modules['gdal'] = gdal
    # Python GDAL doesn't use exceptions by default.
    gdal.UseExceptions()
except Exception as e:
    warnings.warn('Unable to add gdal to the available modules. ' +
                 f'Failed with the following {e}.')

try:
    from osgeo import ogr
    available_modules['ogr'] = ogr
except Exception as e:
    warnings.warn('Unable to add ogr to the available modules. ' +
                 f'Failed with the following {e}.')


def gdal_translate(dest, src, noData=0, *args, **kwargs):
    try:
        # If outputType is specified, convert it to gdal datatype
        kwargs['outputType'] = gdal.GetDataTypeByName(kwargs['outputType'])
    except KeyError:
        # If outputType not specified, no conversion is necessary and GDAL will
        #  use default arguments.
        pass
    opts = gdal.TranslateOptions(noData=noData, *args, **kwargs)
    return gdal.Translate(dest, src, options=opts)


def gdal_polygonize(input_file, output_name, mask='default', *args, **kwargs):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    src_ds = gdal.Open(input_file)
    src_band = src_ds.GetRasterBand(1)

    if mask == 'default':
        mask_band = src_band.GetMaskBand()
    elif mask.lower() == 'none':
        mask_band = None
    else:
        mask_ds = gdal.Open(mask)
        mask_band = mask_ds.GetRasterBand(1)

    srs = src_ds.GetSpatialRef()
    output_datasource = driver.CreateDataSource(output_name)
    out_layer = output_datasource.CreateLayer(output_name, srs=srs)
    field = ogr.FieldDefn('DN', ogr.OFTInteger)
    out_layer.CreateField(field)
    field_id = out_layer.GetLayerDefn().GetFieldIndex('DN')
    return gdal.Polygonize(src_band, mask_band, out_layer, field_id, [], **kwargs)


def ogr2ogr(dest, src, *args, **kwargs):
    srcDS = gdal.OpenEx(src)
    opts = gdal.VectorTranslateOptions(skipFailures=True, *args, **kwargs)
    ds = gdal.VectorTranslate(dest, srcDS=srcDS, options = opts)
    # Dataset isn't written until dataset is closed and dereferenced
    # https://gis.stackexchange.com/questions/255586/gdal-vectortranslate-returns-empty-object
    del ds

def get_single_band_cube(cube,out_cube,band_list,keyname):
    """
    Convenience function to extract a single band from an ISIS cube based on a prioritized list of band numbers,
    and the name of a keyword to search for in the BandBin group of the cube.
    This is necessary for generating browse/thumbnail images from multiband images where certain bands are preferred
    over others for use in the output, but there is no way of knowing whether the preferred band is present without
    inspecting the ingested ISIS cube.

    Parameters
    ----------
    cube : str
             A string file path to the input cube.

    out_cube : str
             A string file path to the desired output cube.

    band_list : list
             A list of ints representing band numbers to search for in cube, in decreasing order of priority

    keyname : str
             The name of the keyword to look for in the BandBin group of the input cube.

    Returns
    -------
    isis.cubeatt() : function
        Calls the ISIS function, cubeatt, in order to write out a single band cube


    """
    bands_in_cube = isis.getkey(from_=cube, objname="IsisCube", grpname="BandBin", keyword=keyname)
    if isinstance(bands_in_cube, bytes):
        bands_in_cube = bands_in_cube.decode()
    bands_in_cube = bands_in_cube.replace('\n', '').replace(' ', '').split(',')
    bands_in_cube = [int(x) for x in bands_in_cube]

    for band in band_list:
        if band in bands_in_cube:
            isis.cubeatt(from_=cube + '+' + str(bands_in_cube.index(band) + 1), to=out_cube)
            break
        else:
            continue

    return


def cube_rename(src, dest):
    """ Thin wrapper to make os.rename available in recipes.

    Parameters
    ----------
    src : str
             A string file path to the file that will be renamed.

    dest : str
             A string file path that serves as the new file path.

    """
    rename(src, dest)
    return
