CREATE TABLE IF NOT EXISTS actions (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  status VARCHAR(50),
  project_id INTEGER,
  system TEXT,
  phase VARCHAR(50),
  date_end TIMESTAMP,
  estimatedtimeinhours DECIMAL(10, 2),
  assigned_to INTEGER,
  rand VARCHAR(50),
  is_deleted TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS project (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  status VARCHAR(50),
  description TEXT,
  date_end TIMESTAMP,
  author_id INTEGER,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS person (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  status VARCHAR(50),
  metadata JSONB,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS company (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email_domains TEXT[],
  deactivated BOOLEAN,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS categories_project (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS checklists (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  status VARCHAR(50),
  imported_uuids TEXT[],
  imported_templates TEXT[],
  project_id INTEGER,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS api_requests (
  id SERIAL PRIMARY KEY,
  method VARCHAR(10) NOT NULL,
  endpoint VARCHAR(255) NOT NULL,
  status_code INTEGER,
  response_time_ms INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);

-- Regression coverage: mixed inline constraints, REFERENCES, CHECK on columns,
-- and a trailing multi-line CONSTRAINT with parenthesized/OR/AND lines (parser
-- must emit only real column names as enum fields).
CREATE TABLE IF NOT EXISTS scoped_document (
  id SERIAL PRIMARY KEY,
  correlation_token UUID NOT NULL UNIQUE,
  owner_profile_id INTEGER NOT NULL REFERENCES person(id) ON DELETE CASCADE,
  company_id INTEGER REFERENCES company(id) ON DELETE CASCADE,
  project_id INTEGER REFERENCES project(id) ON DELETE CASCADE,
  is_workspace_wide BOOLEAN NOT NULL DEFAULT FALSE,
  is_provisional BOOLEAN NOT NULL DEFAULT FALSE,
  tier_label TEXT NOT NULL CHECK (tier_label IN ('core','extended','delegated','system')),
  title JSONB NOT NULL,
  body JSONB NOT NULL,
  appendix JSONB NOT NULL DEFAULT '{}'::jsonb,
  facets JSONB NOT NULL DEFAULT '[]'::jsonb,
  glyph TEXT,
  segment TEXT NOT NULL DEFAULT 'neutral' CHECK (segment IN ('alpha','beta','gamma','general','neutral')),
  opened_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

  CONSTRAINT scoped_document_tier_alignment CHECK (
    (tier_label = 'core' AND project_id IS NULL AND company_id IS NULL AND is_workspace_wide = FALSE)
    OR (tier_label = 'extended'  AND project_id IS NOT NULL AND is_workspace_wide = FALSE)
    OR (tier_label = 'delegated' AND company_id IS NOT NULL AND project_id IS NULL AND is_workspace_wide = FALSE)
    OR (tier_label = 'system'   AND is_workspace_wide = TRUE  AND project_id IS NULL AND company_id IS NULL)
  )
);
