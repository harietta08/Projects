# main.py
# Runs Bronze → Silver → Gold layers end-to-end

import subprocess
import os

scripts = [
    "bronze_layer.py",
    "silver_layer.py",
    "gold_layer.py"
]

print("Running full Medallion Architecture pipeline...")

venv_python = os.path.join("venv", "Scripts", "python.exe")

for script in scripts:
    if os.path.exists(script):
        print(f"Running: {script}")
        subprocess.run([venv_python, script], check=True)
    else:
        print(f"Missing script: {script}")

print("Pipeline complete. Charts saved. Gold layer up-to-date.")
