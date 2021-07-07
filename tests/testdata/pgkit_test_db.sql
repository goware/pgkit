CREATE TABLE accounts (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  disabled BOOLEAN,
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
  etc JSONB
);

CREATE TABLE stats (
  id SERIAL PRIMARY KEY,
  key VARCHAR(80),
  num BIGINT
);
