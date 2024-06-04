##---
# Serves district statistics (population and landuse)
# based on the district data structure
# ---

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
import cgi

params = cgi.FieldStorage()
cityName = params.getvalue('cityname')
if cityName is None:
    cityName = ''

file = open(os.path.dirname(os.path.abspath(__file__)) + "\.pg")
connection_string = file.readline()
pg = psycopg2.connect(connection_string)

records_query = pg.cursor(cursor_factory=RealDictCursor)
records_query.execute("""
    WITH landuse_per_district AS (
        SELECT d.wk_code,
            regexp_replace(d.wk_name, '(^Wijk.{4})', '') AS wk_name,
            l.group_2012,
            sum(ST_Area(ST_Intersection(l.geom, d.geom))) AS use_m2
        FROM netherlands.landuse AS l JOIN netherlands.district AS d
            ON ST_Intersects(l.geom, d.geom)
        WHERE d.gm_naam = '%s'
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
    ),
    series AS (
        SELECT d.wk_code AS code, 'g' || generate_series(1,9) AS group,
            substring(d.wk_code from 7 for 2) AS label,
            regexp_replace(d.wk_name, '(^Wijk.{4})', '') AS name,
            d.aant_inw::REAL AS pop_2020,
            st_area(d.geom) / 1000000 AS area_km2
        FROM  netherlands.district as d
        WHERE d.gm_naam = '%s'
    )
    SELECT s.code, s.label, s.name, s.pop_2020, s.area_km2,
        json_agg((
            s.group,
            coalesce(round(l.use_m2::NUMERIC, 2), 0),
            coalesce(round((l.use_m2 * 100 / (s.area_km2 * 1000000))::NUMERIC, 2), 0)
        ) ORDER BY s.group) AS landuse_2012
    FROM series as s LEFT JOIN landuse_per_district  as l
        ON (l.wk_code = s.code) AND ('g' || l.group_2012 = s.group)
    GROUP BY 1,2,3,4,5;
""" % (cityName, cityName))

results = json.dumps(records_query.fetchall())
results = results.replace('"f1"','"group"').replace('"f2"','"m2"').replace('"f3"','"pct"')

print ("Content-type: application/json")
print ()
print (results)