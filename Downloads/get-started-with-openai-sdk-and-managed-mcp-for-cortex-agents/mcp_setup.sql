CREATE OR REPLACE SECURITY INTEGRATION SALES_MCP_OAUTH
    TYPE = OAUTH
    ENABLED = TRUE
    OAUTH_CLIENT = CUSTOM
    OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
    OAUTH_REDIRECT_URI = 'http://localhost:3000/oauth/callback'
    OAUTH_ALLOW_NON_TLS_REDIRECT_URI = TRUE  -- Allow HTTP for localhost development
    OAUTH_ISSUE_REFRESH_TOKENS = TRUE
    OAUTH_REFRESH_TOKEN_VALIDITY = 86400
    COMMENT = 'OAuth integration for Sales Intelligence MCP Server - Local Development';

-- Retrieve OAuth client credentials (SAVE THESE!)
SELECT SYSTEM$SHOW_OAUTH_CLIENT_SECRETS('SALES_MCP_OAUTH');

-- ============================================
-- CREATE MCP SERVER WITH TOOLS
-- ============================================

-- Create the MCP Server that exposes Cortex Agent
CREATE OR REPLACE MCP SERVER SALES_INTELLIGENCE_MCP
    FROM SPECIFICATION $$
    tools:
      # Cortex Agent for sales intelligence
      - name: "sales-intelligence-agent"
        type: "CORTEX_AGENT_RUN"
        identifier: "SALES_INTELLIGENCE.DATA.SALES_AGENT"
        description: "AI agent for sales intelligence that can search conversations, analyze metrics, and query sales data"
        title: "Sales Intelligence Agent"
      # Semantic view
      - name: "sales-metrics-semantic-view"
        type: "CORTEX_ANALYST_MESSAGE"
        identifier: "SALES_INTELLIGENCE.DATA.SALES_METRICS_SEMANTIC_VIEW"
        description: "Semantic view for sales performance metrics and analytics"
        title: "Sales Metrics Semantic View"
    $$;

-- Grant permissions
GRANT USAGE ON MCP SERVER SALES_INTELLIGENCE.DATA.SALES_INTELLIGENCE_MCP TO ROLE SALES_INTELLIGENCE_ROLE;

-- Grant access to OAuth integration
GRANT USAGE ON INTEGRATION SALES_MCP_OAUTH TO ROLE SALES_INTELLIGENCE_ROLE;

-- Verify the configuration
DESC SECURITY INTEGRATION SALES_MCP_OAUTH;

-- View the created MCP server
SHOW MCP SERVERS IN SCHEMA SALES_INTELLIGENCE.DATA;

-- Describe the MCP server to see its configuration
DESCRIBE MCP SERVER SALES_INTELLIGENCE_MCP;