from common.mappers import keycloak
from common.runtime import serve

if __name__ == "__main__":
    serve("KEYCLOAK", "governance.keycloak.v1", keycloak)
