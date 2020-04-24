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


def gdal_translate(dest, src, **kwargs):
    options = gdal.TranslateOptions([], **kwargs)
    return gdal.Translate(dest, src, options)


def gdal_polygonize(band, mask, output_name, **kwargs):
    driver = ogr.GetDriverByName("ESRI Shapefile")
    output_datasource = driver.CreateDataSource(output_name + ".shp")
    out_layer = output_datasource.CreateLayer(output_name, srs=None)
    field = ogr.FieldDefn('MYFLD', ogr.OFTInteger)
    out_layer.CreateField(field)
    return gdal.Polygonize(band, mask, out_layer, -1, [], **kwargs)


def ogr2ogr(dest, src, **kwargs):
    srcDS = gdal.OpenEx(src)
    vto = gdal.VectorTranslateOptions(**kwargs)
    ds = gdal.VectorTranslate(dest, srcDS=srcDS, options = vto)
    # Dataset isn't written until dataset is closed and dereferenced
    # https://gis.stackexchange.com/questions/255586/gdal-vectortranslate-returns-empty-object
    del ds
