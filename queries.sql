DROP VIEW IF EXISTS tmp;
/*DROP TABLE IF EXISTS trajectoriesTest;
DROP TABLE IF EXISTS categoriesTest;

CREATE TABLE categoriesTest (
  venueid       TEXT NOT NULL,
  venuecategory TEXT,
  latitude      DOUBLE PRECISION,
  longitude     DOUBLE PRECISION,
  cattype       TEXT,
  PRIMARY KEY (venueid)
);
CREATE TABLE trajectoriesTest (
  userid       INTEGER,
  venueid      TEXT,
  utctimestamp TIMESTAMP(6) WITHOUT TIME ZONE,
  tpos         BIGINT
);

INSERT INTO categoriesTest (venueid, venuecategory, latitude, longitude, cattype)
VALUES ('0', 'McDonalds', 0, 0, 'Restaurant');
INSERT INTO categoriesTest
VALUES ('1', 'Juan', 0, 0, 'Home');
INSERT INTO categoriesTest
VALUES ('2', 'Aeroparque', 0, 0, 'Airport');
INSERT INTO categoriesTest
VALUES ('3', 'Ezeiza', 0, 0, 'Airport');
INSERT INTO categoriesTest
VALUES ('4', 'Hilton', 0, 0, 'Hotel');
INSERT INTO categoriesTest
VALUES ('5', 'Superchino', 0, 0, 'Market');
INSERT INTO categoriesTest
VALUES ('6', 'Mati', 0, 0, 'Home');
INSERT INTO categoriesTest
VALUES ('7', 'YPF', 0, 0, 'Station');

INSERT INTO trajectoriesTest
VALUES (1, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (1, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (1, '5', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (1, '3', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (1, '6', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 5);
INSERT INTO trajectoriesTest
VALUES (1, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 6);
INSERT INTO trajectoriesTest
VALUES (1, '1', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 7);
INSERT INTO trajectoriesTest
VALUES (1, '0', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 8);
INSERT INTO trajectoriesTest
VALUES (1, '5', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 9);
INSERT INTO trajectoriesTest
VALUES (1, '3', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 10);
INSERT INTO trajectoriesTest
VALUES (1, '6', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 11);
INSERT INTO trajectoriesTest
VALUES (1, '1', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 12);
INSERT INTO trajectoriesTest
VALUES (2, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (2, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (2, '5', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (2, '2', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (2, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 5);
INSERT INTO trajectoriesTest
VALUES (3, '2', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 6);
INSERT INTO trajectoriesTest
VALUES (3, '4', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 7);
INSERT INTO trajectoriesTest
VALUES (3, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 8);
INSERT INTO trajectoriesTest
VALUES (3, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 9);
INSERT INTO trajectoriesTest
VALUES (3, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 10);
INSERT INTO trajectoriesTest
VALUES (3, '4', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 11);
INSERT INTO trajectoriesTest
VALUES (3, '3', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 12);
INSERT INTO trajectoriesTest
VALUES (3, '4', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 13);
INSERT INTO trajectoriesTest
VALUES (4, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 10);
INSERT INTO trajectoriesTest
VALUES (4, '6', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 11);
INSERT INTO trajectoriesTest
VALUES (4, '3', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 12);
INSERT INTO trajectoriesTest
VALUES (5, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (5, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (5, '6', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (5, '3', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (6, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (6, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (6, '3', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (6, '7', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (7, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (7, '1', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (7, '7', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (7, '3', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (7, '0', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 5);
INSERT INTO trajectoriesTest
VALUES (8, '0', to_timestamp('2018-11-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (8, '1', to_timestamp('2018-11-11 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (8, '7', to_timestamp('2018-12-01 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (8, '3', to_timestamp('2018-12-21 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (9, '0', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (9, '1', to_timestamp('2018-12-03 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (9, '7', to_timestamp('2018-12-04 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (9, '3', to_timestamp('2018-12-05 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);
INSERT INTO trajectoriesTest
VALUES (9, '1', to_timestamp('2018-12-03 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 5);
INSERT INTO trajectoriesTest
VALUES (9, '7', to_timestamp('2018-12-04 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 6);
INSERT INTO trajectoriesTest
VALUES (9, '3', to_timestamp('2018-12-05 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 7);
INSERT INTO trajectoriesTest
VALUES (9, '1', to_timestamp('2018-12-04 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 8);
INSERT INTO trajectoriesTest
VALUES (9, '3', to_timestamp('2018-12-05 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 9);
INSERT INTO trajectoriesTest
VALUES (10, '0', to_timestamp('2018-12-02 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 1);
INSERT INTO trajectoriesTest
VALUES (10, '1', to_timestamp('2018-12-03 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 2);
INSERT INTO trajectoriesTest
VALUES (10, '6', to_timestamp('2018-12-04 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 3);
INSERT INTO trajectoriesTest
VALUES (10, '3', to_timestamp('2018-12-04 11:11:11', 'YYYY-MM-DD hh24:mi:ss'), 4);*/

CREATE VIEW tmp AS (SELECT t.userid, t.venueid, t.tpos, utctimestamp, c.venuecategory, c.cattype
                    FROM trajectoriesTest AS t
                           LEFT JOIN categoriesTest as c ON t.venueid = c.venueid);
/* QUERY 1*/
/*WITH RECURSIVE q1 AS (SELECT userid, ARRAY[cattype] AS cattypePath, utctimestamp, tpos, ARRAY[tpos] AS tposPath
                      FROM tmp

    UNION
    SELECT t.userid, t.cattype || cattypePath, t.utctimestamp, t.tpos, t.tpos || tposPath
    FROM tmp as t
           INNER JOIN q1 AS q ON t.userid = q.userid
    WHERE (q.tpos - 1 = t.tpos))

SELECT q1.userid, q1.tposPath
FROM q1
WHERE q1.cattypePath = ARRAY['Home','Station','Airport'];*/

/* QUERY 2*/

/*WITH RECURSIVE q2 AS (SELECT userid, ARRAY[venuecategory] AS path, ARRAY[cattype] AS cattypePath, utctimestamp, tpos
                      FROM tmp

    UNION
    SELECT t.userid, t.venuecategory || path, t.cattype || cattypePath, t.utctimestamp, t.tpos
    FROM tmp as t
           INNER JOIN q2 AS q ON t.userid = q.userid
    WHERE DATE_PART('day', t.utctimestamp - q.utctimestamp) = 0
      AND (q.tpos - 1 = t.tpos)
      )

SELECT q2.userid, q2.utctimestamp, max(q2.cattypePath) AS cattypePath
FROM q2
WHERE ARRAY['Home', 'Airport'] <@ q2.cattypePath
  AND array_position(q2.cattypePath, 'Home') < array_position(q2.cattypePath, 'Airport')
GROUP BY q2.userid, q2.utctimestamp;*/

/* QUERY 3*/

/*WITH RECURSIVE q3 AS (SELECT userid, ARRAY[venueid] AS path, utctimestamp, tpos, array_length(ARRAY[venueid], 1) as len
                      FROM tmp

    UNION

    SELECT t.userid, t.venueid || path, t.utctimestamp, t.tpos, array_length(t.venueid || path, 1) as len
    FROM tmp as t
           INNER JOIN q3 AS q ON t.userid = q.userid
    WHERE DATE_PART('day', t.utctimestamp - q.utctimestamp) = 0
      AND (q.tpos - 1 = t.tpos)),
               maximals AS (SELECT userid, utctimestamp, MAX(len) as max_len
                            FROM q3
                            WHERE array_length(path, 1) > 1
                            GROUP BY userid, utctimestamp)

SELECT q.userid, q.utctimestamp, q.path
FROM q3 as q
       LEFT JOIN maximals as m on m.userid = q.userid AND m.utctimestamp = q.utctimestamp
WHERE q.len = m.max_len
  AND path [ 1 ] = path [ array_length(path, 1) ];*/

/* QUERY 4*/

/*WITH RECURSIVE q4 AS (SELECT userid, ARRAY[venuecategory] AS path, utctimestamp, tpos
                      FROM tmp

    UNION
    SELECT t.userid, t.venuecategory || path, t.utctimestamp, t.tpos
    FROM tmp as t
           INNER JOIN q4 AS q ON t.userid = q.userid
    WHERE DATE_PART('day', t.utctimestamp - q.utctimestamp) = 0
      AND (q.tpos - 1 = t.tpos))

SELECT q4.userid, q4.utctimestamp, q4.path [ 1 ] as StopInicial, q4.path [ traylength ] as StopFinal
FROM q4
       INNER JOIN (SELECT q4.userid, max(array_length(q4.path, 1)) as traylength
                   FROM q4
                   GROUP BY q4.userid) AS innerq4 ON q4.userid = innerq4.userid
WHERE array_length(q4.path, 1) = innerq4.traylength
ORDER BY q4.userid;*/