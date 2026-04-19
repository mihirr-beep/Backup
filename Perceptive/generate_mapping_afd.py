"""
Master ETL Pipeline Runner
======================================================================
This script orchestrates the execution of the two ETL phases:
  1. a.py -> Generates the wildcard mapping template.
  2. b.py -> Resolves the wildcards against actual patient data.
"""

import subprocess
import sys
import time

def run_script(script_name):
    """Runs a python script and checks for errors."""
    print(f"\n{'='*60}")
    print(f"🚀 STARTING: {script_name}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    
    try:
        # sys.executable ensures it uses the exact same Python (and virtual environment) 
        # that is running this master script.
        result = subprocess.run(
            [sys.executable, script_name],
            check=True,          # Will raise an exception if the script fails
            text=True            # Captures output as strings instead of bytes
        )
        
        elapsed_time = time.time() - start_time
        print(f"\n✅ SUCCESS: {script_name} completed in {elapsed_time:.2f} seconds.")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ERROR: {script_name} failed with exit code {e.returncode}.")
        print("Pipeline aborted.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"\n❌ ERROR: Could not find the script '{script_name}' in the current directory.")
        sys.exit(1)

def main():
    print("🌟 INITIALIZING MASTER ETL PIPELINE 🌟")
    
    # Step 1: Generate the Blueprint
    # (Extracts metadata, creates wildcards, saves Crf_Observation_Data.csv)
    run_script('a.py')
    
    # Step 2: Resolve and Map the Data
    # (Matches wildcards against database, saves Resolved Map & Patient Data)
    run_script('b.py')
    
    print(f"\n{'='*60}")
    print("🎉 ALL PHASES COMPLETED SUCCESSFULLY! 🎉")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()