from common.mappers import kubernetes_audit
from common.runtime import serve

if __name__ == "__main__":
    serve("KUBERNETES", "governance.kubernetes.v1", kubernetes_audit)
