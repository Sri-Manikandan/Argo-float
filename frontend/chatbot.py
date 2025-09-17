import streamlit as st
import httpx
from typing import Dict, Any
from googletrans import Translator


class Chatbot:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.messages = st.session_state.get("messages", [])
        self.translator = Translator()
        st.sidebar.title("🌐 Language")
        self.lang = st.sidebar.selectbox(
            "Select Language", ["English", "Hindi", "Tamil", "Spanish"]
        )
        self.lang_map = {"English": "en", "Hindi": "hi", "Tamil": "ta", "Spanish": "es"}

    def t(self, text: str, to_lang: str = None) -> str:
        try:
            if to_lang is None:
                to_lang = self.lang_map.get(self.lang, "en")
            if to_lang == "en":
                return text
            translated = self.translator.translate(text, dest=to_lang)
            return translated.text
        except Exception as e:
            st.error(f"Translation error: {e}")
            return text

    def display_message(self, message: Dict[str, Any]):
        if message["role"] == "user" and isinstance(message["content"], str):
            st.chat_message("user").write(self.t(message["content"]))
        if message["role"] == "assistant" and isinstance(message["content"], str):
            st.chat_message("assistant").write(self.t(message["content"]))

    async def get_tools(self) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{self.api_url}/tools")
            return response.json()

    async def render(self):
        st.title(self.t("ARGO Chatbot"))
        st.subheader(self.t("Ask questions and get answers to the ARGO oceanography data"))
        for message in st.session_state.get("messages", []):
            self.display_message(message)
        query = st.chat_input(self.t("Ask a question"))
        if query:
            query_in_english = self.t(query, to_lang="en")
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    f"{self.api_url}/query",
                    json={"query": query_in_english},
                    timeout=120.0,
                )
                if response.status_code == 200:
                    messages = response.json()["messages"]
                    st.session_state["messages"] = messages
                    for message in messages:
                        self.display_message(message)