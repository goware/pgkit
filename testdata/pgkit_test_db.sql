CREATE TABLE accounts (
  id serial primary key,
  name varchar(255),
  disabled boolean,
  created_at timestamp with time zone
);

CREATE TABLE review (
  id serial primary key,
  -- article_id integer,
  name varchar(80),
  comments text,
  created timestamp without time zone
);

CREATE TABLE logs (
  id serial primary key,
  message VARCHAR,
  etc jsonb
);
