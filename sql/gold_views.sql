-- Gold Layer Views for Financial Risk Analytics

-- Portfolio Exposure View
CREATE OR REPLACE VIEW vw_portfolio_exposure AS
SELECT 
    product_type,
    risk_segment,
    SUM(outstanding_balance) AS total_exposure,
    COUNT(DISTINCT account_id) AS total_accounts
FROM fact_transactions
GROUP BY product_type, risk_segment;

-- Revenue Analysis View
CREATE OR REPLACE VIEW vw_revenue_analysis AS
SELECT 
    product_type,
    SUM(interest_accrued) AS total_interest,
    SUM(fee_amount) AS total_fees
FROM fact_transactions
GROUP BY product_type;

-- Delinquency Summary View
CREATE OR REPLACE VIEW vw_delinquency_summary AS
SELECT 
    product_type,
    dpd_bucket,
    COUNT(DISTINCT account_id) AS delinquent_accounts,
    SUM(overdue_amount) AS total_overdue
FROM fact_delinquency
GROUP BY product_type, dpd_bucket;