SQL_FILES := $(sort $(wildcard *.sql))

sql_queries_data.inc: $(SQL_FILES) sql2c.awk
	awk -f sql2c.awk $(SQL_FILES) > $@

gen-sql: sql_queries_data.inc

clean:
	rm -f sql_queries_data.inc

.PHONY: gen-sql clean
