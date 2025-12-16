import os
from opensearchpy import OpenSearch, RequestsHttpConnection

# Pull these from env or Django settings for safety
# OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "159.65.149.241")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "127.0.0.1")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", 9200))
OPENSEARCH_USER = os.getenv("OPENSEARCH_USER", "admin")
OPENSEARCH_PASS = os.getenv("OPENSEARCH_PASS", "Bismillah@123")

print(OPENSEARCH_HOST)
client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASS),
    use_ssl=False,
    verify_certs=False,                 # set True and point to CA if you have one
    connection_class=RequestsHttpConnection,
    timeout=30
)
