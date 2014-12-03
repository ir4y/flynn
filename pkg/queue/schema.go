package queue

import (
	"bytes"
	"text/template"

	"github.com/flynn/flynn/pkg/postgres"
)

var stmtTemplates = []*template.Template{
	template.Must(template.New("queue-sql-1").Parse(`
CREATE TABLE {{ .Table }} (
  id bigserial PRIMARY KEY,
  q_name text not null check (length(q_name) > 0),
  data text not null check (length(data) > 0),
  locked_at timestamptz,
  locked_by integer,
  created_at timestamptz default now()
);`[1:])),
	template.Must(template.New("queue-sql-2").Parse(`
CREATE OR REPLACE FUNCTION lock_head(q_name varchar, top_boundary integer)
RETURNS SETOF {{ .Table }} AS $$
DECLARE
  unlocked bigint;
  relative_top integer;
  job_count integer;
BEGIN
  -- The purpose is to release contention for the first spot in the table.
  -- The select count(*) is going to slow down dequeue performance but allow
  -- for more workers. Would love to see some optimization here...

  EXECUTE 'SELECT count(*) FROM '
    || '(SELECT * FROM {{ .Table }} '
    || ' WHERE locked_at IS NULL'
    || ' AND q_name = '
    || quote_literal(q_name)
    || ' LIMIT '
    || quote_literal(top_boundary)
    || ') limited'
  INTO job_count;

  SELECT TRUNC(random() * (top_boundary - 1))
  INTO relative_top;

  IF job_count < top_boundary THEN
    relative_top = 0;
  END IF;

  LOOP
    BEGIN
      EXECUTE 'SELECT id FROM {{ .Table }} '
        || ' WHERE locked_at IS NULL'
        || ' AND q_name = '
        || quote_literal(q_name)
        || ' ORDER BY id ASC'
        || ' LIMIT 1'
        || ' OFFSET ' || quote_literal(relative_top)
        || ' FOR UPDATE NOWAIT'
      INTO unlocked;
      EXIT;
    EXCEPTION
      WHEN lock_not_available THEN
        -- do nothing. loop again and hope we get a lock
    END;
  END LOOP;

  RETURN QUERY EXECUTE 'UPDATE {{ .Table }} '
    || ' SET locked_at = (CURRENT_TIMESTAMP),'
    || ' locked_by = (select pg_backend_pid())'
    || ' WHERE id = $1'
    || ' AND locked_at is NULL'
    || ' RETURNING *'
  USING unlocked;

  RETURN;
END;
$$ LANGUAGE plpgsql;`[1:])),
	template.Must(template.New("queue-sql-3").Parse(`
CREATE OR REPLACE FUNCTION lock_head(tname varchar)
RETURNS SETOF {{ .Table }} AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT * FROM lock_head($1,10)' USING tname;
END;
$$ LANGUAGE plpgsql;`[1:])),
	template.Must(template.New("queue-sql-4").Parse(`
create function {{ .Table }}_notify() returns trigger as $$ begin
  perform pg_notify(new.q_name, '');
  return null;
  end $$ language plpgsql;`[1:])),
	template.Must(template.New("queue-sql-5").Parse(`
create trigger {{ .Table }}_notify
after insert on {{ .Table }}
for each row
execute procedure {{ .Table }}_notify();`[1:])),
}

func (q *Queue) SetupDB() error {
	m := postgres.NewMigrations()
	stmts := make([]string, len(stmtTemplates))
	for i, tmpl := range stmtTemplates {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, q); err != nil {
			return err
		}
		stmts[i] = buf.String()
	}
	m.Add(1, stmts...)
	return m.Migrate(q.DB)
}
