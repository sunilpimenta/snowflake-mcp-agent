import os
import asyncio
import traceback
import httpx
import webbrowser
from typing import TypedDict, Sequence, Optional
from dotenv import load_dotenv
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END, START

load_dotenv()

class AgentState(TypedDict):
    """ State for agent workflow"""
    messages: Sequence[BaseMessage]
    available_tools: list
    mcp_session_id: Optional[str]

class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP Handler to capture OAuth callback"""

    authorization_code = None

    def do_GET(self):
        """Handle GET request to capture authorization code"""
        query_components = parse_qs(urlparse(self.path).query)

        if 'code' in query_components:
            OAuthCallbackHandler.authorization_code = query_components['code'][0]

            # success response
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
                <html>
                    <head><title>Authentication Successful</title></head>
                    <body>
                        <h1>Authentication Successful</h1>
                        <p>You can close this window now.</p>
                    </body>
                </html>
            """)
        else:
            # error response
            self.send_response(400)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
                <html>
                    <head><title>Authentication Failed</title></head>
                    <body>
                        <h1>Authentication Failed</h1>
                        <p>No authorization code received. Please try again.</p>
                    </body>
                </html>
            """)

    def log_message(self, format, *args):
        """ Supress server log messages """
        pass


class SnowflakeMCPClient:
    """MCP Client with OAuth 2.0 authentication for Snowflake Managed MCP Server"""

    def __init__(self):
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.oauth_client_id = os.getenv("OAUTH_CLIENT_ID")
        self.oauth_client_secret = os.getenv("OAUTH_CLIENT_SECRET")
        self.database = os.getenv("SNOWFLAKE_DATABASE", "SALES_INTELLIGENCE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA", "DATA")
        self.mcp_server_name = os.getenv("MCP_SERVER_NAME", "SALES_INTELLIGENCE_MCP")
        self.role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

        self.redirect_uri = "http://localhost:3000/oauth/callback"
        self.access_token = None
        self.base_url = None
        self.session_id = 0
        self.tools = []
        self.llm = None
        self.graph = None

    def setup_connection(self):
        """Setup connection parameters to Snowflake MCP Server"""
        self.base_url = (
            f"https://{self.account}.snowflakecomputing.com"
            f"/api/v2/databases/{self.database}/schemas/{self.schema}/mcp-servers/{self.mcp_server_name}"
        )

        print(f"MCP Server URL: {self.base_url}")

    async def authenticate(self):
        """
        Complete OAuth 2.0 authentication flow
        1. Open browser for user authorization
        2. Start local server to capture callback
        3. Exchange authorization code for access token
        """
        print("\n Starting OAuth 2.0 authentication...")
        print(f"    Client ID: {self.oauth_client_id}")
        print(f"    Redirect URI: {self.redirect_uri}")
        print(f"    Role: {self.role}")

        # Step 1: Build authorization URL
        auth_params = {
            "client_id": self.oauth_client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri
        }
        auth_url = f"https://{self.account}.snowflakecomputing.com/oauth/authorize?{urlencode(auth_params)}"
        
        # Step 2: Open browser for user authorization
        webbrowser.open(auth_url)

        # Step 3: Start local server to capture callback
        print(f" Waiting for authorization (check your browser)...")

        server = HTTPServer(('localhost', 3000), OAuthCallbackHandler)

        server.handle_request()  # handle a single request

        authorization_code = OAuthCallbackHandler.authorization_code

        if not authorization_code:
            raise Exception("Authorization code not received.")
        
        print(f" Authorization code received: {authorization_code}")

        # Step 4: Exchange authorization code for access token
        print(" Exchanging authorization code for access token...")
        
        token_url = f"https://{self.account}.snowflakecomputing.com/oauth/token-request"

        async with httpx.AsyncClient() as client:
            response = await client.post(
                token_url,
                data={
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "redirect_uri": self.redirect_uri,
                    "client_id": self.oauth_client_id,
                    "client_secret": self.oauth_client_secret
                },
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"
                }
            )

            if response.status_code != 200:
                raise Exception(f"Token exchange failed: {response.status_code} - {response.text}")
            token_data = response.json()
            self.access_token = token_data.get("access_token")

        print(" Access token obtained successfully.")
        print(f"    Token expires in: {token_data.get('expires_in')} seconds\n")

    def setup_llm(self):
        """ Initialize LLM for agent orchestration using Snowflake Cortex"""
        print(" Initializing LLM with Snowflake Cortex...\n")

        cortex_base_url = f"https://{self.account}.snowflakecomputing.com/api/v2/cortex/v1"

        # use the OAuth token we already obtained
        self.llm = ChatOpenAI(
            api_key=self.access_token,
            base_url=cortex_base_url,
            model="openai-gpt-5",
            default_headers={
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "X-Snowflake-Role": self.role
            }
        )        
        print(" LLM initialized with Snowflake Cortex (open-ai-gpt-5).\n")

    async def initialize_mcp_session(self):
        """Initialize MCP session with Snowflake MCP Server"""
        print(" Initializing MCP session...")
         
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.base_url, 
                json = {
                    "jsonrpc": "2.0",
                    "id": self.session_id,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18"
                    }
                },
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                    "X-Snowflake-Authorization-Token-Type": "OAUTH",
                    "X-Snowflake-Role": self.role
                }
            )
            if response.status_code != 200:
                raise Exception(f"MCP session initialization failed: {response.status_code} - {response.text}") 
            
            result = response.json()

            if "error" in result:
                raise Exception(f"MCP Error: {result['error']}")
            
            print(f" MCP session initialized successfully. Session ID: {self.session_id}\n")

            if 'result' in result and isinstance(result['result'], dict):
                if 'serverInfo' in result['result']:
                    server_info = result['result']['serverInfo']
                    print(f" Server: {server_info.get('name', 'unknown')}")
                    print(f" Version: {server_info.get('version', 'unknown')}") 
                elif 'server_info' in result['result']:
                    server_info = result['result']['server_info']
                    print(f" Server: {server_info.get('name', 'unknown')}")
                    print(f" Version: {server_info.get('version', 'unknown')}") 
            print()

            self.session_id += 1

            return result
    
    async def discover_tools(self):
        """Discover tools available in the MCP server"""
        print(" Discovering tools...\n")

        async with httpx.AsyncClient() as client:
            response = await client.post (
                self.base_url,
                json = {
                    "jsonrpc": "2.0",
                    "id": self.session_id,
                    "method": "tools/list",
                    "params": {}
                },
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                    "X-Snowflake-Authorization-Token-Type": "OAUTH",
                    "X-Snowflake-Role": self.role
                }
            )

            if response.status_code != 200:
                raise Exception(f"Tool discovery failed: {response.status_code} - {response.text}")
            
            result = response.json()

            if "error" in result:
                raise Exception(f"MCP Error: {result['error']}")
            
            self.tools = result["result"]["tools"]
            self.session_id += 1

            print(f" Discovered {len(self.tools)} tools:\n")
            for tool in self.tools:
                print(f"  - {tool["name"]}")
                print(f"    {tool["description"][:300]}...")
                #print(tool["inputSchema"])
                #if "outputSchema" in tool:
                #    print(tool["outputSchema"])

            return self.tools

    async def call_tool(self, tool_name: str, arguments: dict):
        """Call a tool via MCP server"""
        print(f"\n Calling tool: {tool_name}")
        print(f"    Arguments: {arguments}\n")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post (
                self.base_url,
                json = {
                    "jsonrpc": "2.0",
                    "id": self.session_id,
                    "method": "tools/call",
                    "params": {
                        "name": tool_name,
                        "arguments": arguments
                    }
                },
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                    "X-Snowflake-Authorization-Token-Type": "OAUTH",
                    "X-Snowflake-Role": self.role
                }
            )

            if response.status_code != 200:
                raise Exception(f"Tool call failed: {response.status_code} - {response.text}")
            
            result = response.json()

            if "error" in result:
                raise Exception(f"Tool call error: {result['error']}")
            
            self.session_id += 1

            return result["result"] 

    async def agent_node(self, state: AgentState) -> AgentState:
        """ Agent reasoning node """
        print(" Agent processing...")
        messages = state["messages"]
        available_tools = state["available_tools"]
        
        system_msg = SystemMessage(content=f"""You are a helpful assistant who can answer generic questions. """ )

        response = await self.llm.ainvoke([system_msg] + messages)

        # In a real implementation, you would:
        # 1. Parse if LLM wants to call a tool
        # 2. Use call_tool() method
        # 3. Add tool results back to conversation
        # 4. Continue the loop

        print(f"\n LLM: {response.content}\n")
        return {
            "messages": messages + [response],
            "available_tools": available_tools,
            "mcp_session_id": state.get("mcp_session_id")
        }

    async def should_continue(self, state: AgentState) -> str:
        """ Decide workflow continuation based on agent state """
        
        user_input = input(" Ask more ('y' to quit) --> ").strip().lower()
        if user_input in ['y', 'yes']:
            return "end"
        else: 
            state["messages"].append(HumanMessage(content=user_input))
        return "continue"
    

    async def create_workflow(self):
        """ Create LangGraph workflow"""
        print(" Building LangGraph workflow...\n")

        workflow = StateGraph(AgentState)
        workflow.add_node("agent", self.agent_node)
        workflow.add_edge(START, "agent")
        workflow.add_conditional_edges( 
            "agent",
            self.should_continue,
            {"continue": "agent", "end": END}
        )
        self.graph = workflow.compile()
        print(" Workflow built\n")

    async def interactive_session(self):
        """ Interactive chat """
        print("="*60)
        print(" Snowflake MCP Agent Ready!")
        print("="*60)
        print("\nCommands:")
        print("    'exit' - Quit the session")
        print("\nType your question or command:\n")

        conversation_history = []
        while True:
            try: 
                user_input = input("You: ")

                if user_input.lower() in ['exit', 'quit', 'q']:
                    print("\n Goodbye!")
                    break

                conversation_history.append(HumanMessage(content=user_input))

                state: AgentState = {
                    "messages": conversation_history,
                    "available_tools": self.tools,
                    "mcp_session_id": str(self.session_id)
                }
                result = await self.graph.ainvoke(state)
                final_message = result["messages"][-1]
                conversation_history.append(final_message) 

                print(f"\n Agent: {final_message.content}\n")

            except KeyboardInterrupt:
                print("\n Goodbye!")
                break
            except Exception as e:
                print(f"\n Error: {str(e)}\n")
        
        #dump conversation history
        print("\n Conversation History:")
        for conv in conversation_history:
            print(f"{conv.__class__.__name__}: {conv.content}")

async def main():
    """Main function"""
    print("="*60)
    print("My Snowflake MCP Client with OAuth 2.0")
    print("="*60)

    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "OAUTH_CLIENT_ID",
        "OAUTH_CLIENT_SECRET"
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"\nMissing required environment variables:")
        for var in missing_vars:
            print(f"    - {var}")
        print("\nPlease set these in your .env file")
        return
    
    client = SnowflakeMCPClient()

    try:
        # setup connection
        client.setup_connection()

        # OAuth authentication
        await client.authenticate()

        # initialize MCP
        await client.initialize_mcp_session()
        await client.discover_tools()

        # setup LLM and workflow
        client.setup_llm()
        await client.create_workflow()

        # run tests
        #print("\n Would you like to run tool tests? (y/n): ", end="")
        #if input().strip().lower() == "y":
        #    await client.test_tools()

        # interactive session
        await client.interactive_session()

    except Exception as e:
        print(f"\nError: {str(e)}")
        import traceback
        import json
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())