import sys
import os

# Automatically add the worker directory to sys.path for all pytest runs
worker_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "api_gateway", "worker")
)
if worker_dir not in sys.path:
    sys.path.insert(0, worker_dir)
