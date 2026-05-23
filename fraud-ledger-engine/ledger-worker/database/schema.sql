-- ============================================================================
-- SQL Schema for Real-Time Fraud Detection & Distributed Ledger Engine
-- Standard: PostgreSQL 16
-- Highlights: Double-entry ledger architecture, strict decimal precision, 
--             ACID-compliant check constraints, index optimization, and idempotency.
-- ============================================================================

-- Enable UUID extension if needed
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Clean existing tables (for development/re-initialization convenience)
DROP TABLE IF EXISTS ledger_entries CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS idempotency_keys CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;

-- 1. ACCOUNTS TABLE
-- Stores the high-level balance for each user/merchant.
-- Numeric(20, 4) avoids all floating-point rounding errors (crucial for financial data).
CREATE TABLE accounts (
    id VARCHAR(64) PRIMARY KEY, -- Can be User ID or Merchant ID
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    balance NUMERIC(20, 4) NOT NULL DEFAULT 0.0000,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_balance_non_negative CHECK (balance >= 0.0000)
);

-- Index for speedy lookups on accounts (though PK is already indexed)
CREATE INDEX idx_accounts_currency ON accounts(currency);

-- 2. IDEMPOTENCY KEYS TABLE
-- Prevents double-processing of charge requests.
-- A payment request is uniquely identified by its transaction ID (TxnID / Idempotency Key).
CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(64) PRIMARY KEY, -- Maps directly to Ingestion TxnID
    status VARCHAR(20) NOT NULL,              -- 'PROCESSING', 'COMPLETED', 'FAILED'
    response_code INT,                        -- HTTP response status code to return on duplicate
    response_body TEXT,                       -- Cached response body
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index to prune old idempotency keys if needed
CREATE INDEX idx_idempotency_keys_created_at ON idempotency_keys(created_at);

-- 3. TRANSACTIONS TABLE
-- Represents the raw payment charge request.
-- Status transitions: PENDING -> APPROVED / DECLINED / FRAUD_REJECTED
CREATE TABLE transactions (
    id VARCHAR(64) PRIMARY KEY, -- TxnID
    user_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    amount NUMERIC(20, 4) NOT NULL,
    device_id VARCHAR(128) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    fraud_score NUMERIC(5, 4), -- Store the ML fraud probability
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_transaction_amount_positive CHECK (amount > 0.0000)
);

CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

-- 4. LEDGER ENTRIES TABLE (Journal Entries)
-- Implements Immutable Double-Entry Ledger Bookkeeping.
-- For every approved transaction, there MUST be two matching rows:
-- - A DEBIT from the payer's account (decrease)
-- - A CREDIT to the payee/merchant account (increase)
CREATE TABLE ledger_entries (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL REFERENCES transactions(id) ON DELETE RESTRICT,
    account_id VARCHAR(64) NOT NULL REFERENCES accounts(id) ON DELETE RESTRICT,
    direction VARCHAR(6) NOT NULL, -- 'DEBIT' (money leaving), 'CREDIT' (money entering)
    amount NUMERIC(20, 4) NOT NULL,
    balance_after NUMERIC(20, 4) NOT NULL, -- Historical audit snapshot of balance after this entry
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_entry_amount_positive CHECK (amount > 0.0000),
    CONSTRAINT chk_entry_direction CHECK (direction IN ('DEBIT', 'CREDIT'))
);

CREATE INDEX idx_ledger_entries_transaction_id ON ledger_entries(transaction_id);
CREATE INDEX idx_ledger_entries_account_id ON ledger_entries(account_id);
CREATE INDEX idx_ledger_entries_created_at ON ledger_entries(created_at);


-- ============================================================================
-- SEED DATA
-- Pre-seeding user accounts and a system merchant/treasury account for testing.
-- ============================================================================

-- 1. System Merchant Treasury Account (receives all processed funds)
INSERT INTO accounts (id, currency, balance, created_at, updated_at)
VALUES ('merchant_treasury', 'USD', 1000000.0000, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;

-- 2. Mock User Accounts
INSERT INTO accounts (id, currency, balance, created_at, updated_at)
VALUES 
    ('user_alice', 'USD', 5000.0000, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('user_bob',   'USD', 150.0000,  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('user_charlie', 'USD', 20000.0000, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('user_david', 'USD', 0.0000,     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;
