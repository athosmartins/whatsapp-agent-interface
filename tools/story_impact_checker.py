#!/usr/bin/env python3
"""
Story Impact Checker

Analyzes code changes to determine if they might impact previously solved
problems documented in USER_STORIES.md. Warns developers when modifying
code areas that previous stories addressed.
"""

import os
import sys
import re
import subprocess
from typing import List, Dict, Set, Tuple

class StoryImpactChecker:
    """Checks if code changes might impact previously solved problems."""
    
    def __init__(self, project_root: str = None):
        if project_root is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.project_root = project_root
        self.user_stories_path = os.path.join(project_root, 'USER_STORIES.md')
        
    def get_changed_files(self) -> List[str]:
        """Get list of files changed in the current commit."""
        try:
            # Get staged files for commit
            result = subprocess.run(
                ['git', 'diff', '--cached', '--name-only'],
                capture_output=True,
                text=True,
                cwd=self.project_root
            )
            if result.returncode == 0:
                return [f for f in result.stdout.strip().split('\n') if f and f.endswith('.py')]
            
            # Fallback: get all modified files
            result = subprocess.run(
                ['git', 'diff', '--name-only', 'HEAD~1', 'HEAD'],
                capture_output=True,
                text=True,
                cwd=self.project_root
            )
            if result.returncode == 0:
                return [f for f in result.stdout.strip().split('\n') if f and f.endswith('.py')]
                
        except Exception as e:
            print(f"Warning: Could not detect changed files: {e}")
            
        return []
    
    def load_story_metadata(self) -> Dict[str, Dict]:
        """Load metadata about each story from USER_STORIES.md."""
        stories = {}
        
        if not os.path.exists(self.user_stories_path):
            return stories
            
        try:
            with open(self.user_stories_path, 'r') as f:
                content = f.read()
            
            # Extract story sections
            story_pattern = r'## Story #(\d+): (.+?)\n(.*?)(?=## Story #|\Z)'
            matches = re.findall(story_pattern, content, re.DOTALL)
            
            for story_num, title, body in matches:
                # Extract key information from story
                files_mentioned = self._extract_files_from_story(body)
                functions_mentioned = self._extract_functions_from_story(body)
                keywords = self._extract_keywords_from_story(body)
                
                stories[story_num] = {
                    'title': title.strip(),
                    'files': files_mentioned,
                    'functions': functions_mentioned,
                    'keywords': keywords,
                    'body': body
                }
                
        except Exception as e:
            print(f"Warning: Could not parse USER_STORIES.md: {e}")
            
        return stories
    
    def _extract_files_from_story(self, story_body: str) -> Set[str]:
        """Extract file names mentioned in a story."""
        files = set()
        
        # Look for file patterns
        file_patterns = [
            r'`([^`]+\.py)`',          # `filename.py`
            r'`([^`]+/[^`]+\.py)`',    # `path/filename.py`
            r'([a-zA-Z_][a-zA-Z0-9_]*\.py)',  # filename.py
            r'`([^`]+_loader\.py)`',   # *_loader.py files
            r'`([^`]+_filter\.py)`',   # *_filter.py files
        ]
        
        for pattern in file_patterns:
            matches = re.findall(pattern, story_body)
            files.update(matches)
            
        return files
    
    def _extract_functions_from_story(self, story_body: str) -> Set[str]:
        """Extract function names mentioned in a story."""
        functions = set()
        
        # Look for function patterns
        function_patterns = [
            r'`([a-zA-Z_][a-zA-Z0-9_]*)\(\)`',      # `function_name()`
            r'def ([a-zA-Z_][a-zA-Z0-9_]*)\(',       # def function_name(
            r'`([a-zA-Z_][a-zA-Z0-9_]*)\(`',         # `function_name(`
        ]
        
        for pattern in function_patterns:
            matches = re.findall(pattern, story_body)
            functions.update(matches)
            
        return functions
    
    def _extract_keywords_from_story(self, story_body: str) -> Set[str]:
        """Extract important keywords from a story."""
        keywords = set()
        
        # Important technical keywords
        technical_terms = [
            'SELECT \\*', 'essential_columns', 'memory', 'optimization',
            'filter', 'cache', 'performance', 'crash', 'loading',
            'bairros', 'mega_data', 'columns', 'DuckDB', 'Parquet'
        ]
        
        for term in technical_terms:
            if re.search(term, story_body, re.IGNORECASE):
                keywords.add(term.lower().replace('\\\\', ''))
                
        return keywords
    
    def check_impact(self, changed_files: List[str], stories: Dict[str, Dict]) -> List[Dict]:
        """Check if changed files might impact previous stories."""
        impacts = []
        
        for file_path in changed_files:
            # Check each story for potential impact
            for story_num, story_data in stories.items():
                impact = self._check_file_story_impact(file_path, story_num, story_data)
                if impact:
                    impacts.append(impact)
                    
        return impacts
    
    def _check_file_story_impact(self, file_path: str, story_num: str, story_data: Dict) -> Dict:
        """Check if a specific file change might impact a specific story."""
        reasons = []
        risk_level = 'low'
        
        # Check if file is directly mentioned in story
        file_name = os.path.basename(file_path)
        if any(mentioned_file in file_path for mentioned_file in story_data['files']):
            reasons.append(f"File directly mentioned in Story #{story_num}")
            risk_level = 'high'
        
        # Check for function name matches
        if os.path.exists(os.path.join(self.project_root, file_path)):
            try:
                with open(os.path.join(self.project_root, file_path), 'r') as f:
                    file_content = f.read()
                
                # Check if file contains functions mentioned in story
                for func in story_data['functions']:
                    if f'def {func}(' in file_content:
                        reasons.append(f"Contains function '{func}' mentioned in Story #{story_num}")
                        risk_level = 'high'
                
                # Check for keyword matches
                for keyword in story_data['keywords']:
                    if keyword.lower() in file_content.lower():
                        reasons.append(f"Contains keyword '{keyword}' from Story #{story_num}")
                        if risk_level == 'low':
                            risk_level = 'medium'
                            
            except Exception:
                pass
        
        # Specific high-risk patterns
        if 'mega_data' in file_path and story_num == '001':
            reasons.append("Memory optimization file (Story #001 critical)")
            risk_level = 'high'
        
        if 'filter' in file_path and story_num == '002':
            reasons.append("Filter optimization file (Story #002 critical)")
            risk_level = 'high'
            
        if reasons:
            return {
                'file': file_path,
                'story_number': story_num,
                'story_title': story_data['title'],
                'risk_level': risk_level,
                'reasons': reasons
            }
            
        return None
    
    def generate_impact_report(self, impacts: List[Dict]) -> str:
        """Generate a human-readable impact report."""
        if not impacts:
            return "✅ No story impacts detected. Changes appear safe."
        
        # Group by risk level
        high_risk = [i for i in impacts if i['risk_level'] == 'high']
        medium_risk = [i for i in impacts if i['risk_level'] == 'medium']
        low_risk = [i for i in impacts if i['risk_level'] == 'low']
        
        report = "🔍 STORY IMPACT ANALYSIS\n\n"
        
        if high_risk:
            report += "🚨 HIGH RISK CHANGES:\n"
            for impact in high_risk:
                report += f"   • {impact['file']} → Story #{impact['story_number']}: {impact['story_title']}\n"
                for reason in impact['reasons']:
                    report += f"     - {reason}\n"
                report += "\n"
        
        if medium_risk:
            report += "⚠️  MEDIUM RISK CHANGES:\n"
            for impact in medium_risk:
                report += f"   • {impact['file']} → Story #{impact['story_number']}: {impact['story_title']}\n"
            report += "\n"
        
        if low_risk:
            report += "ℹ️  LOW RISK CHANGES:\n"
            for impact in low_risk:
                report += f"   • {impact['file']} → Story #{impact['story_number']}\n"
            report += "\n"
        
        if high_risk:
            report += "⚠️  RECOMMENDATION: Review USER_STORIES.md entries for affected stories\n"
            report += "    before proceeding to ensure no regressions are introduced.\n"
        
        return report
    
    def run_check(self) -> bool:
        """Run the complete story impact check. Returns True if safe to proceed."""
        print("🔍 Analyzing story impact of code changes...")
        
        changed_files = self.get_changed_files()
        if not changed_files:
            print("ℹ️  No Python files changed. Skipping story impact check.")
            return True
        
        print(f"📁 Analyzing {len(changed_files)} changed files...")
        
        stories = self.load_story_metadata()
        if not stories:
            print("⚠️  Could not load USER_STORIES.md. Proceeding without story impact analysis.")
            return True
        
        impacts = self.check_impact(changed_files, stories)
        report = self.generate_impact_report(impacts)
        
        print(report)
        
        # High risk changes require explicit confirmation in CI/pre-commit
        high_risk_impacts = [i for i in impacts if i['risk_level'] == 'high']
        if high_risk_impacts:
            print("🚨 HIGH RISK: Changes affect critical story areas. Extra caution required.")
            # In automated environments, we warn but don't block
            # In interactive environments, we could prompt for confirmation
            return True
        
        return True

def main():
    """CLI interface for story impact checker."""
    checker = StoryImpactChecker()
    success = checker.run_check()
    
    if not success:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()