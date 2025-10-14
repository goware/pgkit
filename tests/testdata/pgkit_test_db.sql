CREATE TABLE accounts (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  disabled BOOLEAN,
  new_column_not_in_code BOOLEAN, -- test for backward-compatible migrations, see https://github.com/goware/pgkit/issues/13
  created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE articles (
  id SERIAL PRIMARY KEY,
  author VARCHAR(80) NOT NULL,
  alias VARCHAR(80),
  content JSONB,
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP WITHOUT TIME ZONE NULL
);

CREATE TABLE reviews (
  id SERIAL PRIMARY KEY,
  article_id INTEGER REFERENCES articles(id),
  account_id INTEGER NOT NULL REFERENCES accounts(id),
  comment TEXT,
  status SMALLINT,
  sentiment SMALLINT,
  processed_at TIMESTAMP WITHOUT TIME ZONE NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
  deleted_at TIMESTAMP WITHOUT TIME ZONE NULL
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
