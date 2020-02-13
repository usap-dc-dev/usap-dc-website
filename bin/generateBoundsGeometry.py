import psycopg2
import psycopg2.extras
import json

config = json.loads(open('config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def makeGeom(record):
    north = record['north']
    east = record['east']
    west = record['west']
    south = record['south']
    cross_dateline = record['cross_dateline']

    # point
    if (west == east and north == south):
        geom = "POINT(%s %s)" % (west, north)

    # polygon
    else:
        geom = "POLYGON(("
        n = 10
        if (cross_dateline):
            dlon = (-180 - west)/n
            dlat = (north - south)/n
            for i in range(n):
                geom += "%s %s," % (-180-dlon*i, north)

            for i in range(n):
                geom += "%s %s," % (west, north-dlat*i)

            for i in range(n):
                geom += "%s %s," % (west+dlon*i, south)

            dlon = (180 - east)/n
            for i in range(n):
                geom += "%s %s," % (180-dlon*i, south)

            for i in range(n):
                geom += "%s %s," % (east, south+dlat*i)

            for i in range(n):
                geom += "%s %s," % (east+dlon*i, north)
            # close the ring ???
            geom += "%s %s," % (-180, north)

        elif east > west:
            dlon = (west - east)/n
            dlat = (north - south)/n
            for i in range(n):
                geom += "%s %s," % (west-dlon*i, north)

            for i in range(n):
                geom += "%s %s," % (east, north-dlat*i)

            for i in range(n):
                geom += "%s %s," % (east+dlon*i, south)

            for i in range(n):
                geom += "%s %s," % (west, south+dlat*i)
            # close the ring
            geom += "%s %s," % (west, north)

        else:
            dlon = (-180 - east)/n
            dlat = (north - south)/n
            for i in range(n):
                geom += "%s %s," % (-180-dlon*i, north)

            for i in range(n):
                geom += "%s %s," % (east, north-dlat*i)

            for i in range(n):
                geom += "%s %s," % (east+dlon*i, south)

            dlon = (180 - west)/n
            for i in range(n):
                geom += "%s %s," % (180-dlon*i, south)

            for i in range(n):
                geom += "%s %s," % (west, south+dlat*i)

            for i in range(n):
                geom += "%s %s," % (west+dlon*i, north)
            # close the ring ???
            geom += "%s %s," % (-180, north)

        geom = geom[:-1] + "))"
    return geom


if __name__ == '__main__':
    conn, cur = connect_to_db()
    query = "SELECT * FROM dataset_spatial_map"
    cur.execute(query)
    records = cur.fetchall()
    for record in records:
        geom = makeGeom(record)
        update = "UPDATE dataset_spatial_map SET bounds_geometry = ST_GeomFromText('%s',4326) WHERE dataset_id='%s' AND gid=%s AND north='%s' AND south='%s' AND east='%s' AND west='%s';" \
            % (geom, record['dataset_id'], record['gid'], record['north'], record['south'], record['east'], record['west'])
        print(update)
        cur.execute(update)
        cur.execute("COMMIT;")
    print("DONE")
