import psycopg2
import psycopg2.extras
import json

config = json.loads(open('/web/usap-dc/htdocs/config.json', 'r').read())


def connect_to_db():
    info = config['DATABASE']
    conn = psycopg2.connect(host=info['HOST'],
                            port=info['PORT'],
                            database=info['DATABASE'],
                            user=info['USER_CURATOR'],
                            password=info['PASSWORD_CURATOR'])
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    return (conn, cur)


def makeCentroidGeom(record):

    north = record['north']
    east = record['east']
    west = record['west']
    south = record['south']
    cross_dateline = record['cross_dateline']

    mid_point_lat = (south - north) / 2 + north

    if cross_dateline:
        mid_point_long = west - (west - east + 360)/2
    else:
        mid_point_long = (east - west) / 2 + west
        if (west > east):
            mid_point_long += 180

    if mid_point_long < -180:
        mid_point_long += 360
    elif mid_point_long > 180:
        mid_point_long -= 360

    # if geometric bound describe a circle or a ring, set the centroid at the south pole
    if (north != south) and ((round(west, 2) == -180 and round(east, 2) == 180) or (east == west)):
        mid_point_lat = -89.999
        mid_point_long = 0

    geom = "POINT(%s %s)" % (mid_point_long, mid_point_lat)
    return geom


if __name__ == '__main__':
    conn, cur = connect_to_db()
    query = "SELECT * FROM dataset_spatial_map"
    cur.execute(query)
    records = cur.fetchall()
    for record in records:
        geom = makeCentroidGeom(record)
        update = "UPDATE dataset_spatial_map SET geometry = ST_GeomFromText('%s',4326) WHERE dataset_id='%s' AND gid=%s AND north='%s' AND south='%s' AND east='%s' AND west='%s';" \
            % (geom, record['dataset_id'], record['gid'], record['north'], record['south'], record['east'], record['west'])
        print(update)
        cur.execute(update)
        cur.execute("COMMIT;")

    query = "SELECT * FROM project_spatial_map"
    cur.execute(query)
    records = cur.fetchall()
    for record in records:
        geom = makeCentroidGeom(record)
        update = "UPDATE project_spatial_map SET geometry = ST_GeomFromText('%s',4326) WHERE proj_uid='%s' AND gid=%s AND north='%s' AND south='%s' AND east='%s' AND west='%s';" \
            % (geom, record['proj_uid'], record['gid'], record['north'], record['south'], record['east'], record['west'])
        print(update)
        cur.execute(update)
        cur.execute("COMMIT;")

    query = "SELECT * FROM dif_spatial_map"
    cur.execute(query)
    records = cur.fetchall()
    for record in records:
        geom = makeCentroidGeom(record)
        update = "UPDATE dif_spatial_map SET geometry = ST_GeomFromText('%s',4326) WHERE dif_id='%s' AND gid=%s AND north='%s' AND south='%s' AND east='%s' AND west='%s';" \
            % (geom, record['dif_id'], record['gid'], record['north'], record['south'], record['east'], record['west'])
        print(update)
        cur.execute(update)
        cur.execute("COMMIT;")

    query = """REFRESH MATERIALIZED VIEW project_view; 
        REFRESH MATERIALIZED VIEW dataset_view;
        COMMIT;"""
    cur.execute(query)
    print("DONE")
