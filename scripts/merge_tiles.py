#!/usr/bin/env python
import os
import re
import sys
from optparse import OptionParser
from OpenImageIO import ImageBuf, ImageSpec, ImageBufAlgo


def parseOptions():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)
    parser.add_option("-x", "--tile_x", type=int, help="X tile.")
    parser.add_option("-y", "--tile_y", type=int, help="Y tile.")
    parser.add_option("-f", '--frame', type=int, help='Frame number.')
    parser.add_option("-o", '--output', type='string', help='Output joined image.')
    parser.add_option("-m", '--filemask', type='string', help='Python syntax formatting mask for find tiles. For instance /mnt/render/test1.hip_c9Dm_mantra1.mantra1_tile%%02d_.%%04d.exr.')

    (opts, args) = parser.parse_args(sys.argv[1:])
    return opts, args



def main():
    options, args = parseOptions()

    tile_x = options.tile_x
    tile_y = options.tile_y
    frame = options.frame
    output = options.output
    filemask = options.filemask

    tile_files = []
    tiles_lost = []
    for i in xrange(0, (tile_x*tile_y)):
        filepath = filemask % (i,frame)
        if not os.path.exists(filepath):
            tiles_lost += [filepath]
            continue
        tile_files += [ filepath ]

    if len(tile_files) != (tile_x*tile_y):
        raise Exception("Tile not found: %s" % tiles_lost)

    #TODO: merge metadata from tiles

    spec = ImageBuf(str(tile_files[0])).spec()
    spec_e = ImageSpec (spec.full_width,spec.full_height,spec.nchannels, spec.format)

    extended = ImageBuf(spec_e)
    for filename in tile_files:
        img = ImageBuf(filename)
        ImageBufAlgo.paste(extended, img.xbegin, img.ybegin, img.zbegin, 0, img, nthreads=4)
    extended.write( output )



if __name__ == "__main__": main()

