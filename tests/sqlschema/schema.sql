-- Schema for nim_sqlquery test suite

-- Actions table: represents tasks/actions in the system
CREATE TABLE IF NOT EXISTS actions (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  status VARCHAR(50),
  project_id INTEGER,
  phase VARCHAR(50),
  date_end TIMESTAMP,
  estimatedtimeinhours DECIMAL(10, 2),
  assigned_to INTEGER,
  rand VARCHAR(50),
  is_deleted TIMESTAMP DEFAULT NULL
);

-- Project table: represents projects
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

-- Person table: represents users/persons in the system
CREATE TABLE IF NOT EXISTS person (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  status VARCHAR(50),
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);

-- Company table: represents companies
CREATE TABLE IF NOT EXISTS company (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Categories_project table: represents project categories
CREATE TABLE IF NOT EXISTS categories_project (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(50)
);

-- QA Paradigm table: represents checklists
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

-- API requests table: represents HTTP API request logs
-- Note: "method" is a reserved word in many languages, testing column name handling
CREATE TABLE IF NOT EXISTS api_requests (
  id SERIAL PRIMARY KEY,
  method VARCHAR(10) NOT NULL,
  endpoint VARCHAR(255) NOT NULL,
  status_code INTEGER,
  response_time_ms INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_deleted TIMESTAMP DEFAULT NULL
);
