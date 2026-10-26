package sqlengine

import (
	"context"
	"testing"
)

// TestTriggersAndProcedures pins issues #19 and #20 against MySQL 8.0.46's
// results for the same statements.
func TestTriggersAndProcedures(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE t (id int PRIMARY KEY, v int)", "[]"},
		{"CREATE TABLE log (id int AUTO_INCREMENT PRIMARY KEY, msg varchar(100))", "[]"},
		{"CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v = NEW.v * 10", "[]"},
		{"CREATE TRIGGER t_ai AFTER INSERT ON t FOR EACH ROW INSERT INTO log (msg) VALUES (CONCAT('ins ', NEW.id, ' ', NEW.v))", "[]"},
		{"CREATE TRIGGER t_bu BEFORE UPDATE ON t FOR EACH ROW BEGIN IF NEW.v < 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'negative'; END IF; END", "[]"},
		{"CREATE TRIGGER t_ad AFTER DELETE ON t FOR EACH ROW INSERT INTO log (msg) VALUES (CONCAT('del ', OLD.id))", "[]"},
		{"INSERT INTO t VALUES (1, 2), (2, 3)", "[]"},
		{"SELECT * FROM t ORDER BY id", "[1,20 2,30]"},
		{"UPDATE t SET v = -1 WHERE id = 1", "ERR 1644"},
		{"DELETE FROM t WHERE id = 2", "[]"},
		{"SELECT msg FROM log ORDER BY id", "[ins 1 20 ins 2 30 del 2]"},
		{"SELECT trigger_name, event_manipulation, action_timing FROM information_schema.triggers ORDER BY trigger_name",
			"[t_ad,DELETE,AFTER t_ai,INSERT,AFTER t_bi,INSERT,BEFORE t_bu,UPDATE,BEFORE]"},
		{"CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.v = 1", "ERR 1359"},
		{"DROP TRIGGER t_bi", "[]"},
		{"DROP TRIGGER t_bi", "ERR 1360"},
		{"DROP TRIGGER IF EXISTS t_bi", "[]"},

		// an IF that sets NEW and then runs another statement (it panicked)
		{"CREATE TABLE acct (aid int PRIMARY KEY, balance int NOT NULL)", "[]"},
		{"INSERT INTO acct VALUES (1, 1000), (2, 3000)", "[]"},
		{"CREATE TRIGGER acct_bu BEFORE UPDATE ON acct FOR EACH ROW BEGIN DECLARE e varchar(255); IF ABS(NEW.balance - OLD.balance) > 1000 THEN SET NEW.balance := OLD.balance; SET e := CONCAT('Too big change for aid = ', NEW.aid); SET @failed := IF(@failed, CONCAT(@failed, ':', e), e); END IF; END", "[]"},
		{"SET @failed := ''", "[]"},
		{"UPDATE acct SET balance = 1500", "[]"},
		{"SELECT @failed", "[Too big change for aid = 2]"},
		{"SELECT * FROM acct ORDER BY aid", "[1,1500 2,3000]"},

		// trigger schema, temporary tables, NEW/OLD misuse
		{"CREATE DATABASE other", "[]"},
		{"CREATE TABLE other.o (i int)", "[]"},
		{"CREATE TRIGGER o_bi BEFORE INSERT ON other.o FOR EACH ROW SET @a := 1", "ERR 1435"},
		{"CREATE TEMPORARY TABLE tt (i int)", "[]"},
		{"CREATE TRIGGER tt_bi BEFORE INSERT ON tt FOR EACH ROW SET @a := 1", "ERR 1361"},
		{"CREATE TRIGGER x BEFORE INSERT ON t FOR EACH ROW SET @a := OLD.v", "ERR 1363"},

		// a trigger broken by ALTER TABLE fails when it fires, and only then
		{"CREATE TABLE b (i int, bt int)", "[]"},
		{"CREATE TRIGGER b_bi BEFORE INSERT ON b FOR EACH ROW SET @a := NEW.bt", "[]"},
		{"ALTER TABLE b DROP COLUMN bt", "[]"},
		{"INSERT INTO b VALUES (1)", "ERR 1054"},
		{"INSERT INTO log (msg) VALUES ('unaffected')", "[]"},
		{"DROP TABLE b", "[]"},
		{"SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_name = 'b_bi'", "[0]"},

		// procedures
		{"CREATE PROCEDURE p(IN x int, OUT y int) BEGIN SET y = x * 2; END", "[]"},
		{"CALL p(21, @y)", "[]"},
		{"SELECT @y", "[42]"},
		{"CREATE PROCEDURE p() SELECT 1", "ERR 1304"},
		{"CREATE PROCEDURE sumv(OUT s int) BEGIN DECLARE done int DEFAULT 0; DECLARE x int; DECLARE c CURSOR FOR SELECT id FROM t; DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; SET s = 0; OPEN c; l: LOOP FETCH c INTO x; IF done THEN LEAVE l; END IF; SET s = s + x; END LOOP; CLOSE c; END", "[]"},
		{"CALL sumv(@s)", "[]"},
		{"SELECT @s", "[1]"},
		{"SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = 'app'", "[2]"},
		// error handlers (go-mysql-server's interpreter looped forever, or
		// skipped a BEGIN ... END body)
		{"CREATE PROCEDURE h1() BEGIN DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET @h1 = 'caught'; SIGNAL SQLSTATE 'HY000'; SET @after1 = 1; END", "[]"},
		{"CALL h1()", "[]"},
		{"SELECT @h1, @after1", "[caught,1]"},
		{"CREATE PROCEDURE h2() BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET @h2a = 1; SET @h2b = 2; END; INSERT INTO nosuch VALUES (1); SET @after2 = 1; END", "[]"},
		{"CALL h2()", "[]"},
		{"SELECT @h2a, @h2b, @after2", "[1,2,NULL]"},
		{"CREATE TABLE ht (i int PRIMARY KEY)", "[]"},
		{"CREATE PROCEDURE h3() BEGIN DECLARE n int DEFAULT 0; DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET n = n + 1; INSERT INTO ht VALUES (1); INSERT INTO ht VALUES (1); INSERT INTO ht VALUES (2); INSERT INTO ht VALUES (2); SET @n = n; END", "[]"},
		{"CALL h3()", "[]"},
		{"SELECT @n, COUNT(*) FROM ht", "[2,2]"},
		// an EXIT handler of an inner block ends with it (it stayed in force
		// and looped); SELECT ... INTO a user variable
		{"CREATE PROCEDURE h4() BEGIN DECLARE c CURSOR FOR SELECT i FROM ht; OPEN c; BEGIN DECLARE v1 INT; DECLARE v2 INT; DECLARE EXIT HANDLER FOR SQLEXCEPTION SET @h4 = 'err'; WHILE TRUE DO FETCH c INTO v1, v2; END WHILE; END; CLOSE c; SELECT i INTO @foo FROM ht ORDER BY i LIMIT 1; END", "[]"},
		{"CALL h4()", "[]"},
		{"SELECT @h4, @foo", "[err,1]"},
		{"CREATE PROCEDURE q1() BEGIN SELECT i INTO @q FROM ht WHERE i > 100; END", "[]"},
		{"CALL q1()", "[]"},
		{"CREATE PROCEDURE q2() SELECT i INTO @q FROM ht", "[]"},
		{"CALL q2()", "ERR 1172"},
		{"DROP PROCEDURE p", "[]"},
		{"DROP PROCEDURE p", "ERR 1305"},
		{"CALL p(1, @y)", "ERR 1305"},
		{"DROP DATABASE other", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
