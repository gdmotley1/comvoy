"""Dev server launcher — ensures correct working directory."""
import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())

import uvicorn
uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
