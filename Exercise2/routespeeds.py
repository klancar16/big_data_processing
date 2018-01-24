import sys
from PIL import Image, ImageDraw, ImageFont
import math
import json

def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)


def overlap(cur_x, cur_y, t_size, last_x, last_y, last_size):
    x_gap = abs(cur_x - last_x) - t_size[0]/2 - last_size[0]/2
    y_gap = abs(cur_y - last_y) - t_size[1]/2 - last_size[1]/2
    return x_gap <= 1 and y_gap <= 1


def out_of_map(bus_lat, bus_long, start_coor, end_coor):
    return not (start_coor[0] >= bus_lat >= end_coor[0] and
                start_coor[1] <= bus_long <= end_coor[1])


if __name__ == '__main__':
    map_image = sys.argv[1]
    bus_data_file = sys.argv[2]
    (start_x, start_y, end_x, end_y) = (4633, 2307, 4642, 2313)
    ZOOM = 13

    tampere_map = Image.open(map_image)
    start_coor = num2deg(start_x, start_y, ZOOM)
    end_coor = num2deg(end_x, end_y, ZOOM)
    x_span = abs(end_coor[1] - start_coor[1])
    y_span = abs(end_coor[0] - start_coor[0])

    x_map_size = tampere_map.size[0]
    y_map_size = tampere_map.size[1]

    with open(bus_data_file, 'r') as file:
        bus_data = json.load(file)

    bus_idx = 0
    while out_of_map(float(bus_data[bus_idx]['body'][1]['monitoredVehicleJourney']['vehicleLocation']['latitude']),
                     float(bus_data[bus_idx]['body'][1]['monitoredVehicleJourney']['vehicleLocation']['longitude']),
                     start_coor, end_coor):
        bus_idx = bus_idx + 1
    print(bus_idx)
    bus_ref = bus_data[bus_idx]['body'][1]['monitoredVehicleJourney']['vehicleRef']
    origin_dep_time = bus_data[bus_idx]['body'][1]['monitoredVehicleJourney']['originAimedDepartureTime']

    vehicle_journeys = [journey for x in bus_data for journey in x['body']]
    journeys_by_bus = [{'speed': x['monitoredVehicleJourney']['speed'],
                        'location': x['monitoredVehicleJourney']['vehicleLocation']}
                       for x in vehicle_journeys
                       if (x['monitoredVehicleJourney']['vehicleRef'] == bus_ref and
                           x['monitoredVehicleJourney']['originAimedDepartureTime'] == origin_dep_time)]

    dr_img = ImageDraw.Draw(tampere_map)
    fnt = ImageFont.load_default()

    last_text = (-1, -1)
    last_size = (0, 0)
    for bus in journeys_by_bus:
        bus_long = float(bus['location']['longitude'])
        bus_lat = float(bus['location']['latitude'])
        if out_of_map(bus_lat, bus_long, start_coor, end_coor):
            print("out")
            continue
        x_off = abs(bus_long - start_coor[1])
        y_off = abs(bus_lat - start_coor[0])
        x = round((x_off / x_span) * x_map_size)
        y = round((y_off / y_span) * y_map_size)

        speed = bus['speed']
        t_size = dr_img.textsize(text=str(speed), font=fnt)
        cur_x = x - t_size[0] / 2
        cur_y = y - t_size[1] / 2

        if cur_x < 0 or cur_y < 0 or (cur_x + t_size[0]) >= x_map_size or (cur_y + t_size[1]) >= y_map_size:
            continue

        if not overlap(cur_x, cur_y, t_size, last_text[0], last_text[1], last_size):
            last_text = (cur_x, cur_y)
            last_size = t_size
            dr_img.text(xy=(cur_x, cur_y), text=str(speed), fill='black', font=fnt)

    tampere_map.save('data/routespeeds.png')


