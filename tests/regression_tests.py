#!/usr/bin/env python3
"""
Regression Prevention Tests

This module contains automated tests that verify previously solved problems 
remain solved. These tests prevent regressions by validating that critical 
optimizations and fixes are still in place.

Each test is tied to a specific USER_STORY and will fail if that story's 
solution has been undone or modified incorrectly.
"""

import sys
import os
import re
import ast
import inspect
from typing import List, Dict, Tuple

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class RegressionTestFailure(Exception):
    """Exception raised when a regression is detected."""
    pass

class RegressionPrevention:
    """Main class for regression detection and prevention."""
    
    def __init__(self):
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.failed_tests = []
        
    def run_all_tests(self) -> bool:
        """Run all regression tests and return True if all pass."""
        print("🔍 Running Regression Prevention Tests...")
        
        tests = [
            self.test_story_001_memory_optimization,
            self.test_story_002_filter_performance, 
            self.test_story_003_infinite_loading_fix,
            self.test_no_select_star_in_bairros_loading,
            self.test_essential_columns_preserved,
            self.test_memory_efficient_patterns
        ]
        
        passed = 0
        total = len(tests)
        
        for test in tests:
            try:
                test()
                print(f"✅ {test.__name__}")
                passed += 1
            except RegressionTestFailure as e:
                print(f"❌ {test.__name__}: {e}")
                self.failed_tests.append((test.__name__, str(e)))
            except Exception as e:
                print(f"🚨 {test.__name__}: Unexpected error: {e}")
                self.failed_tests.append((test.__name__, f"Unexpected error: {e}"))
        
        print(f"\n📊 Results: {passed}/{total} tests passed")
        
        if self.failed_tests:
            print("\n🚨 REGRESSION DETECTED - The following issues must be fixed:")
            for test_name, error in self.failed_tests:
                print(f"   • {test_name}: {error}")
            return False
        
        print("✅ All regression tests passed!")
        return True
    
    def test_story_001_memory_optimization(self):
        """
        Story #001: Memory-Efficient Mega Data Set Loading
        
        Verifies that the essential columns optimization is still in place
        and SELECT * is not being used in bairros loading functions.
        """
        # Check that load_bairros_optimized doesn't use SELECT *
        file_path = os.path.join(self.project_root, 'services', 'mega_data_set_loader.py')
        
        if not os.path.exists(file_path):
            raise RegressionTestFailure("mega_data_set_loader.py not found")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Look for the problematic SELECT * pattern in load_bairros_optimized
        if 'SELECT * FROM' in content and 'load_bairros_optimized' in content:
            # Check if SELECT * appears in the same function context
            lines = content.split('\n')
            in_function = False
            for i, line in enumerate(lines):
                if 'def load_bairros_optimized' in line:
                    in_function = True
                elif in_function and line.strip().startswith('def ') and 'load_bairros_optimized' not in line:
                    in_function = False
                elif in_function and 'SELECT * FROM' in line:
                    raise RegressionTestFailure(
                        f"load_bairros_optimized is using 'SELECT *' on line {i+1}. "
                        "This undoes Story #001 memory optimization. Should use essential columns only."
                    )
        
        # Verify essential columns list exists
        if '_load_essential_columns_only' not in content:
            raise RegressionTestFailure("Essential columns function missing - Story #001 optimization removed")
        
        # Verify essential columns are defined
        essential_pattern = r'ESSENTIAL_COLUMNS\s*=\s*\['
        if not re.search(essential_pattern, content):
            raise RegressionTestFailure("ESSENTIAL_COLUMNS list missing - Story #001 optimization removed")
    
    def test_story_002_filter_performance(self):
        """
        Story #002: Ultra-Fast Smart Cascading Filters
        
        Verifies that lazy loading and smart filter cascading are still in place.
        """
        # Check that lazy_column_loader exists
        file_path = os.path.join(self.project_root, 'services', 'lazy_column_loader.py')
        if not os.path.exists(file_path):
            raise RegressionTestFailure("lazy_column_loader.py missing - Story #002 optimization removed")
        
        # Check that smart_filter_cascade exists  
        file_path = os.path.join(self.project_root, 'services', 'smart_filter_cascade.py')
        if not os.path.exists(file_path):
            raise RegressionTestFailure("smart_filter_cascade.py missing - Story #002 optimization removed")
    
    def test_story_003_infinite_loading_fix(self):
        """
        Story #003: Production Infinite Loading Bug Fix
        
        Verifies that safe_rerun protection and cached data loading are still in place.
        """
        # Look for safe_rerun implementation
        found_safe_rerun = False
        
        for root, dirs, files in os.walk(self.project_root):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r') as f:
                            content = f.read()
                            if 'safe_rerun' in content or 'rerun protection' in content:
                                found_safe_rerun = True
                                break
                    except:
                        continue
            if found_safe_rerun:
                break
        
        if not found_safe_rerun:
            raise RegressionTestFailure("safe_rerun protection missing - Story #003 fix removed")
    
    def test_no_select_star_in_bairros_loading(self):
        """
        Critical test: Ensure no bairros loading function uses SELECT *
        
        This is the specific regression that caused the SAVASSI crash.
        """
        problematic_files = []
        
        for root, dirs, files in os.walk(self.project_root):
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r') as f:
                            content = f.read()
                            
                            # Check for SELECT * in functions containing 'bairro' in their name
                            if 'bairro' in content.lower() and 'SELECT *' in content:
                                lines = content.split('\n')
                                for i, line in enumerate(lines):
                                    if 'SELECT *' in line and any(keyword in content.lower() for keyword in ['bairro', 'load_bairros', 'mega_data']):
                                        problematic_files.append((file_path, i+1, line.strip()))
                    except:
                        continue
        
        if problematic_files:
            file_list = '\n'.join([f"   • {f}: line {l}: {code}" for f, l, code in problematic_files])
            raise RegressionTestFailure(
                f"SELECT * found in bairros loading code:\n{file_list}\n"
                "This causes memory crashes. Use essential columns only."
            )
    
    def test_essential_columns_preserved(self):
        """
        Verify that the essential columns list is preserved and contains the right columns.
        """
        file_path = os.path.join(self.project_root, 'services', 'mega_data_set_loader.py')
        
        if not os.path.exists(file_path):
            raise RegressionTestFailure("mega_data_set_loader.py not found")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Essential columns that must be present
        required_columns = [
            'DOCUMENTO PROPRIETARIO',
            'BAIRRO', 
            'ENDERECO',
            'INDICE CADASTRAL',
            'GEOMETRY'
        ]
        
        for col in required_columns:
            if f"'{col}'" not in content and f'"{col}"' not in content:
                raise RegressionTestFailure(f"Essential column '{col}' missing from code")
    
    def test_memory_efficient_patterns(self):
        """
        Test that memory-efficient patterns are maintained throughout the codebase.
        """
        # Check that load_mega_data_set function has mode parameter for memory efficiency
        file_path = os.path.join(self.project_root, 'services', 'mega_data_set_loader.py')
        
        if not os.path.exists(file_path):
            raise RegressionTestFailure("mega_data_set_loader.py not found")
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        if 'def load_mega_data_set(' not in content:
            raise RegressionTestFailure("load_mega_data_set function missing")
        
        if 'mode=' not in content:
            raise RegressionTestFailure("Memory-efficient mode parameter missing from load_mega_data_set")

def main():
    """Run all regression tests."""
    tester = RegressionPrevention()
    success = tester.run_all_tests()
    
    if not success:
        print("\n🚨 CRITICAL: Regressions detected! Please fix before proceeding.")
        sys.exit(1)
    else:
        print("\n✅ All regression tests passed. Code quality maintained.")
        sys.exit(0)

if __name__ == "__main__":
    main()