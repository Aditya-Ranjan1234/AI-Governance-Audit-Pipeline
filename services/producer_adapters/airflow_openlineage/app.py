from common.mappers import airflow_openlineage
from common.runtime import serve

if __name__ == "__main__":
    serve("PIPELINE", "governance.pipeline.v1", airflow_openlineage)
