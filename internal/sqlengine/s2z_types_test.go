package sqlengine

import (
	"context"
	"testing"
)

// TestResultTypes pins against MySQL 8.0.46 the types MySQL gives SUM and
// AVG, the TIME functions, COALESCE / CASE / IF over temporal values and
// x + INTERVAL, the values they show, and errors for invalid defaults and
// long identifiers.
func TestResultTypes(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	long := func(c byte, n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = c
		}
		return string(b)
	}
	for _, s := range []struct{ q, want string }{
		// SUM and AVG: DECIMAL of exact numbers, DOUBLE of floats
		{"CREATE TABLE n (i int, d decimal(10,3), b bigint, f float, dt datetime(2), dd date, y year)", "[]"},
		{"INSERT INTO n VALUES (1, 1.5, 2, 1.5, '2001-01-01 10:10:10.5', '2001-01-01', 2001),(2, 2.25, 3, 2.5, '2001-01-01 10:10:10', '2001-01-02', 2002)", "[]"},
		{"CREATE TABLE n2 AS SELECT SUM(i), AVG(i), SUM(d), AVG(d), SUM(b), AVG(b), SUM(f), AVG(f), SUM(dt), AVG(dt), SUM(dd), AVG(dd), SUM(y), AVG(y) FROM n", "[]"},
		{"SHOW CREATE TABLE n2", "[n2,CREATE TABLE `n2` (\n  `SUM(i)` decimal(32,0) DEFAULT NULL,\n  `AVG(i)` decimal(14,4) DEFAULT NULL,\n  `SUM(d)` decimal(32,3) DEFAULT NULL,\n  `AVG(d)` decimal(14,7) DEFAULT NULL,\n  `SUM(b)` decimal(41,0) DEFAULT NULL,\n  `AVG(b)` decimal(23,4) DEFAULT NULL,\n  `SUM(f)` double DEFAULT NULL,\n  `AVG(f)` double DEFAULT NULL,\n  `SUM(dt)` decimal(38,2) DEFAULT NULL,\n  `AVG(dt)` decimal(20,6) DEFAULT NULL,\n  `SUM(dd)` decimal(30,0) DEFAULT NULL,\n  `AVG(dd)` decimal(12,4) DEFAULT NULL,\n  `SUM(y)` decimal(26,0) DEFAULT NULL,\n  `AVG(y)` decimal(8,4) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT SUM(i), AVG(i), SUM(d), AVG(d), SUM(dt), AVG(dt), SUM(dd), AVG(dd), SUM(y), AVG(y) FROM n", "[3,1.5000,3.750,1.8750000,40020202202020.50,20010101101010.250000,40020203,20010101.5000,4003,2001.5000]"},
		{"CREATE TABLE tm (t0 TIME, t3 TIME(3), t6 TIME(6))", "[]"},
		{"INSERT INTO tm VALUES ('80:11:12','80:11:12','80:11:12'),('10:11:12.5','10:11:12.5','10:11:12.5')", "[]"},
		{"SELECT AVG(t0), SUM(t0), AVG(t3), SUM(t3), AVG(t6), SUM(t6) FROM tm", "[451112.5000,902225,451112.2500000,902224.500,451112.2500000000,902224.500000]"},
		// TIME arithmetic shows the type's digits; -x of a TIME is a number
		{"SELECT t3+0, t3*1, -t3, CONCAT(t3+0) FROM tm WHERE t0 > '20:00:00'", "[801112.000,801112.000,-801112.000,801112.000]"},
		// ADDTIME, SUBTIME, TIMEDIFF, SEC_TO_TIME, MAKETIME
		{"CREATE TABLE ta (t0 time, t3 time(3), d2 datetime(2), s varchar(20), dd date, i int)", "[]"},
		{"INSERT INTO ta VALUES ('10:00:00','01:02:03.5','2001-01-01 10:00:00.25','10:00:00.5','2001-01-01',3600)", "[]"},
		{"CREATE TABLE tb AS SELECT addtime(t0,'01:00:00') a1, addtime(t0,t3) a3, addtime(d2,t0) a4, addtime(dd,t0) a8, timediff(t0,t3) b1, timediff(d2,d2) b2, timediff(s,s) b3, subtime(t3, t0) c1, sec_to_time(i) e1, sec_to_time(1.5) e2, maketime(1,2,3.25) e4 FROM ta", "[]"},
		{"SHOW CREATE TABLE tb", "[tb,CREATE TABLE `tb` (\n  `a1` time DEFAULT NULL,\n  `a3` time(3) DEFAULT NULL,\n  `a4` datetime(2) DEFAULT NULL,\n  `a8` datetime DEFAULT NULL,\n  `b1` time(3) DEFAULT NULL,\n  `b2` time(2) DEFAULT NULL,\n  `b3` time(6) DEFAULT NULL,\n  `c1` time(3) DEFAULT NULL,\n  `e1` time DEFAULT NULL,\n  `e2` time(1) DEFAULT NULL,\n  `e4` time(2) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT CONCAT(addtime(t0,t3)), CONCAT(timediff(s,s)), CONCAT(sec_to_time(1.5)), CONCAT(addtime(d2,'1:00')), addtime(t0,t3)+0, SEC_TO_TIME('100'), MAKETIME(1,2,'3.25') FROM ta", "[11:02:03.500,00:00:00.000000,00:00:01.5,2001-01-01 11:00:00.25,110203.500,00:01:40.000000,01:02:03.250000]"},
		// COALESCE, CASE, IF, IFNULL of temporal values
		{"CREATE TABLE tc (t0 TIME, t3 TIME(3), t6 TIME(6), d date, dt datetime(2), ts timestamp(4) null)", "[]"},
		{"INSERT INTO tc VALUES ('10:11:12','10:11:12.5','10:11:12.25','2001-01-01','2001-01-01 10:00:00.5','2001-01-01 10:00:00.1234')", "[]"},
		{"CREATE TABLE td AS SELECT CASE WHEN 1 THEN t0 ELSE t3 END a, COALESCE(t0,t3) b, IF(1,t0,t6) c, IFNULL(t0,t3) e, COALESCE(d,dt) f, IF(0,ts,ts) h, COALESCE(d,d) i, IFNULL(dt,ts) j FROM tc", "[]"},
		{"SHOW CREATE TABLE td", "[td,CREATE TABLE `td` (\n  `a` time(3) DEFAULT NULL,\n  `b` time(3) DEFAULT NULL,\n  `c` time(6) DEFAULT NULL,\n  `e` time(3) DEFAULT NULL,\n  `f` datetime(2) DEFAULT NULL,\n  `h` timestamp(4) NULL DEFAULT NULL,\n  `i` date DEFAULT NULL,\n  `j` datetime(4) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT CONCAT(COALESCE(t0,t3)), CONCAT(IF(1,t0,t6)), CONCAT(COALESCE(d,dt)), CONCAT(d) FROM tc", "[10:11:12.000,10:11:12.000000,2001-01-01 00:00:00.00,2001-01-01]"},
		// string functions show a TIME or DATETIME with its digits
		{"SELECT LENGTH(t3), LENGTH(dt), CHAR_LENGTH(t3), REPLACE(dt,'x','y') FROM tc", "[12,22,12,2001-01-01 10:00:00.50]"},
		// x + INTERVAL: a TIME stays a TIME under a day
		{"CREATE TABLE ti AS SELECT t3 + INTERVAL 1 HOUR h, t3 + INTERVAL 1 DAY d, t3 - INTERVAL 1 MONTH m, t3 + INTERVAL 1 MICROSECOND u FROM tc", "[]"},
		{"SHOW CREATE TABLE ti", "[ti,CREATE TABLE `ti` (\n  `h` time(3) DEFAULT NULL,\n  `d` datetime(3) DEFAULT NULL,\n  `m` datetime(3) DEFAULT NULL,\n  `u` time(6) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci]"},
		{"SELECT CONCAT(h), CONCAT(u), CONCAT(TIME'10:00:00' - INTERVAL 30 HOUR) FROM ti", "[11:11:12.500,10:11:12.500001,-20:00:00]"},
		// a TIME as a DATE or DATETIME is on the statement's date
		{"SET @@timestamp = UNIX_TIMESTAMP('2001-01-01 00:00:00')", "[]"},
		{"CREATE TABLE tt (t6 TIME(6), d DATE, dt6 DATETIME(6))", "[]"},
		{"INSERT INTO tt (t6) VALUES ('10:10:10.1'), ('34:10:10.1')", "[]"},
		{"UPDATE tt SET d=t6, dt6=t6", "[]"},
		{"SELECT CONCAT(d), CONCAT(dt6), CONCAT(CAST(t6 AS DATE)), CONCAT(CAST(t6 AS DATETIME(6))) FROM tt ORDER BY t6", "[2001-01-01,2001-01-01 10:10:10.100000,2001-01-01,2001-01-01 10:10:10.100000 2001-01-02,2001-01-02 10:10:10.100000,2001-01-02,2001-01-02 10:10:10.100000]"},
		{"SET @@timestamp = DEFAULT", "[]"},
		// invalid defaults, too many digits, long names
		{"CREATE TABLE e1 (a int default 100 auto_increment)", "ERR 1067"},
		{"CREATE TABLE e1 (a tinyint default 1000)", "ERR 1067"},
		{"CREATE TABLE e1 (a varchar(5) default 'abcdef')", "ERR 1067"},
		{"CREATE TABLE e1 (a time(7))", "ERR 1426"},
		{"CREATE TABLE e1 (a datetime(9))", "ERR 1426"},
		{"CREATE TABLE `" + long('a', 65) + "` (a int)", "ERR 1059"},
		{"CREATE TABLE e1 (`" + long('a', 65) + "` int)", "ERR 1059"},
		{"CREATE VIEW `" + long('v', 65) + "` AS SELECT 1", "ERR 1059"},
		{"CREATE DATABASE `" + long('b', 65) + "`", "ERR 1059"},
		{"CREATE TABLE `" + long('a', 64) + "` (a int)", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
