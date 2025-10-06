#!/usr/bin/env python3
"""
Quick Test Runner
Runs the test suite and provides a summary
"""

import subprocess
import sys
import time

def run_tests():
    """Run the complete test suite"""
    print("Starting E-commerce Analytics Test Suite")
    print("=" * 60)
    
    try:
        # Run pytest with detailed output
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            "tests/", 
            "-v", 
            "--tb=short",
            "--color=yes"
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        print("TEST RESULTS:")
        print("-" * 40)
        print(result.stdout)
        
        if result.stderr:
            print("\nWARNINGS/ERRORS:")
            print("-" * 40)
            print(result.stderr)
        
        # Parse results
        if "FAILED" in result.stdout:
            print("\nSome tests failed!")
        elif "passed" in result.stdout:
            print("\nAll tests passed!")
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("\nTests timed out (took longer than 5 minutes)")
        return False
    except Exception as e:
        print(f"\nError running tests: {e}")
        return False

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)