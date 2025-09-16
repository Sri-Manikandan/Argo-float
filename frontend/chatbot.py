import streamlit as st
import httpx
from typing import Dict, Any

class Chatbot:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.messages = st.session_state["messages"]

    def display_message(self, message: Dict[str, Any]):
        if message["role"] == "user" and type(message["content"]) == str:
            st.chat_message("user").write(message["content"])

        # if message["role"] == "assistant" and type(message["content"]) == list:
        #     for item in message["content"]:
        #         if item["type"]=="tool_use":
        #             st.chat_message("assistant").json({
        #                 "tool": item["name"],
        #                 "input": item["input"],
        #             }, expanded=False)

        # if message["role"] == "user" and type(message["content"]) == list:
        #     st.chat_message("assistant").json({
        #         "result": message["content"],
        #     }, expanded=False)
        
        if message["role"] == "assistant" and type(message["content"]) == str:
            st.chat_message("assistant").write(message["content"])

        

    async def get_tools(self) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.api_url}/tools")
            return response.json()

    async def render(self):
        st.title("ARGO Chatbot")
        st.subheader("Ask questions and get answers to the ARGO oceanography data")

        # with st.sidebar:
        #     st.subheader("Settings")
        #     st.write("API URL:", self.api_url)
        #     result = await self.get_tools()
        #     st.subheader("Available Tools")
        #     st.write([tool["name"] for tool in result["tools"]])

        for message in st.session_state["messages"]:
            self.display_message(message)

        query = st.chat_input("Ask a question")
        if query:
            # st.chat_message("user").write(query)
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(f"{self.api_url}/query", json={"query": query})
                if response.status_code == 200:
                    messages = response.json()["messages"]
                    st.session_state["messages"] = messages
                    for message in messages:
                        # st.chat_message(message["role"]).write(message["content"])
                        self.display_message(message)

            

