CREATE TABLE accounts (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  disabled BOOLEAN,
  new_column_not_in_code BOOLEAN, -- test for backward-compatible migrations, see https://github.com/goware/pgkit/issues/13
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE reviews (
  id SERIAL PRIMARY KEY,
  -- article_id integer,
  name VARCHAR(80),
  comments TEXT,
  created_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE TABLE logs (
  id SERIAL PRIMARY KEY,
  message VARCHAR,
  raw_data bytea,
  etc JSONB
);

CREATE TABLE stats (
  id SERIAL PRIMARY KEY,
  key VARCHAR(80) UNIQUE,
  big_num NUMERIC(78,0) NOT NULL, -- representing a big.Int runtime type
  rating NUMERIC(78,0) NULL -- representing a nullable big.Int runtime type
);

CREATE TABLE articles (
  id SERIAL PRIMARY KEY,
  author VARCHAR(80) NOT NULL,
  alias VARCHAR(80),
  content JSONB
);
