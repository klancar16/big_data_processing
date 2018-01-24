import sys
import math
from PIL import Image
import io
import urllib.request


def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return (xtile, ytile)


if __name__ == '__main__':
    coor_x_start = float(sys.argv[1])
    coor_y_start = float(sys.argv[2])
    coor_x_end = float(sys.argv[3])
    coor_y_end = float(sys.argv[4])
    output_file = sys.argv[5]

    ZOOM = 13
    init_url = "http://tile.openstreetmap.org/{0}/{1}/{2}.png"  # tile.openstreetmap.org/ZOOM/X/Y.png

    (x_start, y_start) = deg2num(coor_x_start, coor_y_start, ZOOM)
    (x_end, y_end) = deg2num(coor_x_end, coor_y_end, ZOOM)

    tiles = []
    for x in range(x_start, x_end+1):
        row = []
        for y in range(y_start, y_end+1):
            tile = Image.open(io.BytesIO(urllib.request.urlopen(init_url.format(ZOOM, x, y)).read()))
            row.append(tile)
        tiles.append(row)

    height = (y_end+1 - y_start) * 256
    width = (x_end+1 - x_start) * 256
    completeMap = Image.new("RGB", (width, height))

    x = 0
    for row in tiles:
        y = 0
        for tile in row:
            completeMap.paste(tile, (x, y))
            y = y + 256
        x = x + 256
    completeMap.save(output_file)
