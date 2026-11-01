from common.mappers import model_registry
from common.runtime import serve

if __name__ == "__main__":
    serve("MODEL_REGISTRY", "governance.model-registry.v1", model_registry)
