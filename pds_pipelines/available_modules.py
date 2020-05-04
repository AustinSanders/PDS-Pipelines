import warnings
available_modules = {}

try:
    from pysis import isis
    available_modules['isis'] = isis
except Exception as e:
    warnings.warn('Unable to add isis to the available modules. ' +
                 f'Failed with the following {e}.')

try:
    import gdal
    available_modules['gdal'] = gdal
    # Python GDAL doesn't use exceptions by default.
    gdal.UseExceptions()
except Exception as e:
    warnings.warn('Unable to add gdal to the available modules. ' +
                 f'Failed with the following {e}.')

try:
    import ogr
    available_modules['ogr'] = ogr
except Exception as e:
    warnings.warn('Unable to add ogr to the available modules. ' +
                 f'Failed with the following {e}.')


def gdal_translate(dest, src, *args, **kwargs):
    try:
        # If outputType is specified, convert it to gdal datatype
        kwargs['outputType'] = gdal.GetDataTypeByName(kwargs['outputType'])
    except KeyError:
        # If outputType not specified, no conversion is necessary and GDAL will
        #  use default arguments.
        pass
    opts = gdal.TranslateOptions(*args, **kwargs)
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
