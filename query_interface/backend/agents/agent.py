from langchain_openai import ChatOpenAI


class QueryAgent:
    def __init__(self):
        self.llm = self._init_llm()
        self.tools = self._init_tools()
        self.agent_system_prompt = self._init_agent_system_prompt()
        self.react_agent = self._init_react_agent()

    def _init_llm(self):
        from lib.helper import OPENAI_API_KEY

        return ChatOpenAI(
            model="gpt-4o-mini",
            temperature=0.0,
            api_key=OPENAI_API_KEY,
        )

    def _init_tools(self):
        pass

    def _init_agent_system_prompt(self):
        pass

    def _init_react_agent(self):
        pass

    def execute(self, query: str):
        pass
