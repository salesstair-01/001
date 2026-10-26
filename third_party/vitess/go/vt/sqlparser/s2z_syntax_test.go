package sqlparser

import "testing"

// s2z patches: syntax MySQL accepts that upstream vitess refused.
func TestMySQLSyntax(t *testing.T) {
	for in, want := range map[string]string{
		"(select * from t)": "select * from t",
		"(SELECT a FROM t ORDER BY a LIMIT 7) ORDER BY a LIMIT 4":              "select * from (select a from t order by a asc limit 7) as __s2z_paren order by a asc limit 4",
		"(select a from t) order by a":                                         "select a from t order by a asc",
		"drop tables t1, t2":                                                   "drop table t1, t2",
		"lock table t1 read":                                                   "lock tables t1 read",
		"select a is unknown, a is not unknown from t":                         "select a is null, a is not null from t",
		"select std(i) over () std, variance(j) over () variance from t":       "select std(i) over () `std`, variance(j) over () `variance` from t",
		"select a from t use key for join (i1) ignore index for order by (i2)": "select a from t use index (i1)",
		"create table t (a int, key using btree (a))":                          "",
		"create table t (y year(4), b blob(1), c text(100))":                   "",
		"create table t2 (a int) select * from t1":                             "",
		"alter table t add constraint primary key (a)":                         "",
	} {
		st, err := Parse(in)
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got := String(st); want != "" && got != want {
			t.Errorf("%s:\n got %s\nwant %s", in, got, want)
		}
	}
	for in, typ := range map[string]string{"blob(1)": "tinyblob", "blob(255)": "tinyblob", "blob(256)": "blob", "blob(70000)": "mediumblob", "text(63)": "tinytext", "text(64)": "text", "text(20000000)": "longtext"} {
		st, err := Parse("create table t (c " + in + ")")
		if err != nil {
			t.Fatal(err)
		}
		if got := st.(*DDL).TableSpec.Columns[0].Type.Type; got != typ {
			t.Errorf("%s: %s, want %s", in, got, typ)
		}
	}
}
