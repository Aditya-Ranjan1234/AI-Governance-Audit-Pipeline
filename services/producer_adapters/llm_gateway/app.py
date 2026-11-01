from common.mappers import llm_gateway
from common.runtime import serve

if __name__ == "__main__":
    serve("LLM_GATEWAY", "governance.llm-gateway.v1", llm_gateway)
