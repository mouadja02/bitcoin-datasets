#!/usr/bin/env python3
"""
NewHedge Data Pipeline - Fetch, Load, and Export
This script:
1. Fetches data from NewHedge.io using fetch_newhedge.py
2. Loads the data into Snowflake NEWHEDGE schema
3. Exports the updated Snowflake tables back to CSV files
"""

import os
import sys
import subprocess
from pathlib import Path

# Add scripts directory to path
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

def run_step(step_name, command, description):
    """Run a step in the pipeline."""
    print(f"\n{'='*70}")
    print(f"STEP {step_name}: {description}")
    print(f"{'='*70}\n")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            cwd=SCRIPTS_DIR.parent,
            capture_output=False,
            text=True
        )
        print(f"\n✓ {step_name} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {step_name} failed with exit code {e.returncode}")
        return False

def main():
    """Run the complete NewHedge data pipeline."""
    
    print("""
    ╔════════════════════════════════════════════════════════════════╗
    ║           NewHedge Data Pipeline - Full Execution            ║
    ║                                                                ║
    ║  1. Fetch data from NewHedge.io                               ║
    ║  2. Load data into Snowflake NEWHEDGE schema                  ║
    ║  3. Export updated tables to CSV                              ║
    ╚════════════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: Fetch NewHedge data
    success = run_step(
        "1/3",
        "python scripts/fetch_newhedge.py",
        "Fetching data from NewHedge.io"
    )
    
    if not success:
        print("\n❌ Pipeline failed at Step 1")
        return 1
    
    # Step 2: Load data into Snowflake
    success = run_step(
        "2/3",
        "python scripts/load_newhedge_to_snowflake.py",
        "Loading data into Snowflake and exporting tables"
    )
    
    if not success:
        print("\n❌ Pipeline failed at Step 2")
        return 1
    
    print(f"\n{'='*70}")
    print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*70}")
    print("\nData locations:")
    print(f"  - Raw CSV files:     data/newhedge/")
    print(f"  - Exported tables:   data/newhedge_export/")
    print(f"  - Snowflake schema:  NEWHEDGE.*")
    print()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
