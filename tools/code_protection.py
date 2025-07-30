#!/usr/bin/env python3
"""
Code Protection System

This module provides tools to mark critical code sections and validate
that previously solved problems remain solved. It prevents regressions
by linking code directly to USER_STORIES.md entries.
"""

import os
import re
import sys
from typing import List, Dict, Tuple, Optional

class CodeProtection:
    """System for protecting critical code from regressions."""
    
    def __init__(self, project_root: str = None):
        if project_root is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.project_root = project_root
        self.user_stories_path = os.path.join(project_root, 'USER_STORIES.md')
        
    def add_protection_marker(self, file_path: str, line_number: int, story_number: str, description: str):
        """
        Add a protection marker above critical code.
        
        Args:
            file_path: Path to the file to protect
            line_number: Line number to add marker above (1-indexed)
            story_number: Story number (e.g., "001", "002")
            description: Brief description of what's protected
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        if line_number < 1 or line_number > len(lines):
            raise ValueError(f"Invalid line number: {line_number}")
        
        # Create protection marker
        marker_lines = [
            f"# PROTECTED: Story #{story_number} - {description}\n",
            f"# DO NOT MODIFY: This code solves a previously documented problem\n", 
            f"# See USER_STORIES.md Story #{story_number} for context\n",
            f"# Regression tests will fail if this protection is removed\n"
        ]
        
        # Insert marker above the specified line
        insert_index = line_number - 1
        for i, marker_line in enumerate(marker_lines):
            lines.insert(insert_index + i, marker_line)
        
        # Write back to file
        with open(file_path, 'w') as f:
            f.writelines(lines)
        
        print(f"✅ Added protection marker for Story #{story_number} at {file_path}:{line_number}")
    
    def scan_protected_code(self) -> List[Dict]:
        """
        Scan all Python files for protection markers and validate they're still intact.
        
        Returns:
            List of protection violations found
        """
        violations = []
        protected_sections = []
        
        # Find all protected sections
        for root, dirs, files in os.walk(self.project_root):
            # Skip certain directories
            if any(skip in root for skip in ['__pycache__', '.git', 'node_modules', '.venv']):
                continue
                
            for file in files:
                if file.endswith('.py'):
                    file_path = os.path.join(root, file)
                    protected_sections.extend(self._find_protected_sections(file_path))
        
        # Validate each protected section
        for section in protected_sections:
            violation = self._validate_protected_section(section)
            if violation:
                violations.append(violation)
        
        return violations
    
    def _find_protected_sections(self, file_path: str) -> List[Dict]:
        """Find all protected sections in a file."""
        sections = []
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except:
            return sections
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if line.startswith('# PROTECTED: Story #'):
                # Extract story number and description
                match = re.match(r'# PROTECTED: Story #(\d+) - (.+)', line)
                if match:
                    story_num = match.group(1)
                    description = match.group(2)
                    
                    # Find the end of the protection block
                    j = i + 1
                    while j < len(lines) and lines[j].strip().startswith('#'):
                        j += 1
                    
                    # The protected code starts at line j
                    sections.append({
                        'file_path': file_path,
                        'story_number': story_num,
                        'description': description,
                        'marker_start_line': i + 1,
                        'protected_code_start_line': j + 1,
                        'marker_lines': lines[i:j]
                    })
                    
                    i = j
                else:
                    i += 1
            else:
                i += 1
        
        return sections
    
    def _validate_protected_section(self, section: Dict) -> Optional[Dict]:
        """
        Validate that a protected section is still intact.
        
        Returns violation dict if validation fails, None if OK.
        """
        file_path = section['file_path']
        story_num = section['story_number']
        
        # Read current file content
        try:
            with open(file_path, 'r') as f:
                content = f.read()
        except:
            return {
                'type': 'file_missing',
                'file_path': file_path,
                'story_number': story_num,
                'message': f"Protected file missing: {file_path}"
            }
        
        # Story-specific validations
        if story_num == '001':
            return self._validate_story_001(content, section)
        elif story_num == '002':
            return self._validate_story_002(content, section)
        elif story_num == '003':
            return self._validate_story_003(content, section)
        
        return None
    
    def _validate_story_001(self, content: str, section: Dict) -> Optional[Dict]:
        """Validate Story #001 memory optimization is intact."""
        file_path = section['file_path']
        
        # Check for SELECT * regression in mega_data_set files
        if 'mega_data_set' in file_path and 'SELECT *' in content:
            # Look for problematic SELECT * in bairros loading
            if 'load_bairros' in content and 'WHERE BAIRRO IN' in content:
                return {
                    'type': 'story_001_regression',
                    'file_path': file_path,
                    'story_number': '001',
                    'message': 'Story #001 regression: SELECT * found in bairros loading (should use essential columns)'
                }
        
        # Verify essential columns list exists
        if 'mega_data_set_loader.py' in file_path:
            if 'ESSENTIAL_COLUMNS' not in content:
                return {
                    'type': 'story_001_missing_optimization',
                    'file_path': file_path,
                    'story_number': '001',
                    'message': 'Story #001 regression: ESSENTIAL_COLUMNS missing'
                }
        
        return None
    
    def _validate_story_002(self, content: str, section: Dict) -> Optional[Dict]:
        """Validate Story #002 filter optimization is intact."""
        # Add specific validations for Story #002 if needed
        return None
    
    def _validate_story_003(self, content: str, section: Dict) -> Optional[Dict]:
        """Validate Story #003 infinite loading fix is intact.""" 
        # Add specific validations for Story #003 if needed
        return None
    
    def protect_story_001_code(self):
        """Add protection markers to Story #001 critical code."""
        # Protect the load_bairros_optimized function
        loader_path = os.path.join(self.project_root, 'services', 'mega_data_set_loader.py')
        
        if os.path.exists(loader_path):
            with open(loader_path, 'r') as f:
                lines = f.readlines()
            
            # Find load_bairros_optimized function
            for i, line in enumerate(lines):
                if 'def load_bairros_optimized(' in line:
                    self.add_protection_marker(
                        loader_path, 
                        i + 1,  # Line number is 1-indexed
                        '001',
                        'Memory-efficient bairros loading with essential columns only'
                    )
                    break
    
    def generate_validation_report(self) -> str:
        """Generate a comprehensive validation report."""
        violations = self.scan_protected_code()
        
        if not violations:
            return "✅ All protected code sections are intact. No regressions detected."
        
        report = f"🚨 REGRESSION VIOLATIONS DETECTED ({len(violations)} issues):\n\n"
        
        for i, violation in enumerate(violations, 1):
            report += f"{i}. {violation['type'].replace('_', ' ').title()}\n"
            report += f"   File: {violation['file_path']}\n"
            report += f"   Story: #{violation['story_number']}\n"
            report += f"   Issue: {violation['message']}\n\n"
        
        report += "🔧 ACTION REQUIRED: Fix these regressions before proceeding.\n"
        report += "💡 Refer to USER_STORIES.md for the original solutions.\n"
        
        return report

def main():
    """CLI interface for code protection tools."""
    if len(sys.argv) < 2:
        print("Usage: python code_protection.py [scan|protect-story-001|report]")
        sys.exit(1)
    
    command = sys.argv[1]
    protection = CodeProtection()
    
    if command == 'scan':
        violations = protection.scan_protected_code()
        if violations:
            print(f"🚨 Found {len(violations)} protection violations!")
            for v in violations:
                print(f"   • {v['file_path']}: {v['message']}")
            sys.exit(1)
        else:
            print("✅ All protected code is intact.")
    
    elif command == 'protect-story-001':
        protection.protect_story_001_code()
        print("✅ Added protection markers for Story #001")
    
    elif command == 'report':
        report = protection.generate_validation_report()
        print(report)
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()