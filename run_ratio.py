#!/usr/bin/env python

from __future__ import print_function
import os
import re
import argparse
import rasterio
import numpy as np
from osgeo import gdal

GET_BANDS = ['10', '11', '12']
BAND_REGEX = 'HDF4_EOS:EOS_SWATH:.*(?:ImageData|SurfaceRadianceTIR:Band|TIR_Swath:ImageData)([0-9]{1,2})$'

def main(hdf_path, outpath):
    '''
    takes the input hdf path, generates a SO2 band map, and then saves the result to outpath
    '''
    ratio = gen_ratio(hdf_path)
    write_as_tif(ratio, outpath)
    return ratio

def gen_ratio(hdf_path):
    '''
    loads the hdf path, generates the campion array, and returns the result
    '''
    conversion = {}
    #the band scaling factors
    conversion['10'] = 0.001#0.006882
    conversion['11'] = 0.001#0.006780
    conversion['12'] = 0.001#0.006590
    conversion['13'] = 0.001#0.005693
    conversion['14'] = 0.001#0.005225
    radiance = {}
    bands = [10, 11, 12] # bands to load
    print('loading %s' % hdf_path)
    src_ds = gdal.Open(hdf_path)
    sub = src_ds.GetSubDatasets()
    for data in sub:
        subdataset = str(data[0])
        match = re.search(BAND_REGEX, subdataset)
        if not match:
            #print('{} not matching regex'.format(subdataset))
            continue
        band = match.group(1)
        field = match.group(0)
        if band not in GET_BANDS:
            continue
        #print('loading band %s' % band)
        raster = gdal.Open(data[0])
        radiance[str(band)] = np.ma.masked_less_equal(np.array(raster.ReadAsArray().astype(np.float64)),0) * conversion[str(band)]
    ratio = np.ma.subtract(np.ma.add(radiance['10'], radiance['12']), (2  * radiance['11']))
    msk = np.clip(np.ma.getmask(radiance['10']) + np.ma.getmask(radiance['11']) + np.ma.getmask(radiance['12']), 0, 1)
    ratio = np.logical_not(msk) * ratio
    #clip min
    ratio = np.ma.clip(ratio, 0.0, None)
    print('minimum: {}, maximum: {}'.format(np.ma.min(ratio), np.ma.max(ratio)))
    return ratio

def write_as_tif(input_array, outpath):
    '''
    write the input_array to the outpath
    '''
    if os.path.exists(outpath):
        os.remove(outpath)
    driver = gdal.GetDriverByName("GTiff")
    [cols, rows] = input_array.shape
    outdata = driver.Create(outpath, rows, cols, 1, gdal.GDT_Float64)
    outdata.GetRasterBand(1).WriteArray(input_array)
    #outdata.GetRasterBand(1).SetNoDataValue(0)
    outdata.FlushCache()

def parser():
    '''
    Construct a parser to parse arguments
    @return argparse parser
    '''
    parse = argparse.ArgumentParser(description="Generates product from input file")
    parse.add_argument("-f", "--hdf", required=True, help="path of input hdf file", dest="hdf")
    parse.add_argument("-o", "--out", required=True, help="path to output file", dest="out")
    return parse

if __name__ == '__main__':
    args = parser().parse_args()
    main(args.hdf, args.out)
