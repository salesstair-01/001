package sqlengine

import (
	"context"
	"testing"
)

// TestFulltext pins FULLTEXT indexes and MATCH ... AGAINST against MySQL
// 8.0.46: natural language and boolean mode, through INSERT, UPDATE, DELETE
// and ALTER TABLE ADD FULLTEXT; the index's own tables stay hidden.
func TestFulltext(t *testing.T) {
	c := startCluster(t)
	conn, err := c.db(t).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, s := range []struct{ q, want string }{
		{"CREATE TABLE ft (id int PRIMARY KEY, a VARCHAR(200), b TEXT, FULLTEXT (a,b))", "[]"},
		{"INSERT INTO ft VALUES (1,'MySQL tutorial','DBMS stands for DataBase'),(2,'How To Use MySQL Well','After you went through a'),(3,'Optimizing MySQL','In this tutorial we show'),(4,'1001 MySQL Tricks','1. Never run mysqld as root'),(5,'MySQL vs. YourSQL','In the following database comparison'),(6,'MySQL Security','When configured properly, MySQL')", "[]"},
		{"SHOW TABLES", "[ft]"},
		{"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'app'", "[1]"},
		{"SELECT index_name, index_type FROM information_schema.statistics WHERE table_name = 'ft' ORDER BY index_name, seq_in_index", "[a,FULLTEXT a,FULLTEXT PRIMARY,BTREE]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('database') ORDER BY id", "[1 5]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('+MySQL -YourSQL' IN BOOLEAN MODE) ORDER BY id", "[1 2 3 4 6]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('tutorial security' IN BOOLEAN MODE) ORDER BY id", "[1 3 6]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('+tutorial +optimizing' IN BOOLEAN MODE) ORDER BY id", "[3]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('data*' IN BOOLEAN MODE) ORDER BY id", "[1 5]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('\"following database\"' IN BOOLEAN MODE) ORDER BY id", "[5]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('+mysql +(tricks security)' IN BOOLEAN MODE) ORDER BY id", "[4 6]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('-mysql' IN BOOLEAN MODE) ORDER BY id", "[]"},
		{"UPDATE ft SET b = 'nothing here' WHERE id = 5", "[]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('database') ORDER BY id", "[1]"},
		{"DELETE FROM ft WHERE id = 1", "[]"},
		{"SELECT id FROM ft WHERE MATCH (a,b) AGAINST ('database') ORDER BY id", "[]"},
		{"CREATE TABLE k (id int, t TEXT)", "[]"},
		{"INSERT INTO k VALUES (1,'red apple'),(2,'green apple'),(3,'red wine')", "[]"},
		{"ALTER TABLE k ADD FULLTEXT INDEX kt (t)", "[]"},
		{"SELECT id FROM k WHERE MATCH (t) AGAINST ('red') ORDER BY id", "[1 3]"},
		{"DROP TABLE ft, k", "[]"},
		{"SHOW TABLES", "[]"},
	} {
		if got := connRun(t, conn, s.q); got != s.want {
			t.Errorf("%s: got %s, want %s", s.q, got, s.want)
		}
	}
}
