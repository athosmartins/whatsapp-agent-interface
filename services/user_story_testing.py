#!/usr/bin/env python3
"""
Native Streamlit Testing Framework for User Story Workflow
Integrates streamlit.testing.v1.AppTest into the user story completion process
"""

from streamlit.testing.v1 import AppTest
import time
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
import traceback

class UserStoryTester:
    """
    Native Streamlit testing framework for user story verification.
    Uses Streamlit's built-in testing capabilities for reliable component testing.
    """
    
    def __init__(self, story_name: str, story_number: str = None):
        self.story_name = story_name
        self.story_number = story_number or "unknown"
        self.test_results = {
            "story_name": story_name,
            "story_number": story_number,
            "start_time": datetime.now().isoformat(),
            "tests": [],
            "success": False,
            "total_tests": 0,
            "passed_tests": 0,
            "failed_tests": 0
        }
        
    def debug_log(self, message: str):
        """Log debug information with timestamp."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"🧪 [{timestamp}] {message}")
        
    def test_app_component(self, 
                          app_file: str,
                          test_name: str,
                          test_function: Callable[[AppTest], bool],
                          description: str = "") -> bool:
        """
        Test a specific Streamlit app component using native testing.
        
        Args:
            app_file: Path to the Streamlit app file (e.g., "app.py", "pages/Processor.py")
            test_name: Name of the test for reporting
            test_function: Function that takes AppTest instance and returns True/False
            description: Description of what the test verifies
            
        Returns:
            bool: True if test passed, False if failed
        """
        
        self.debug_log(f"Starting test: {test_name}")
        
        test_result = {
            "name": test_name,
            "description": description,
            "app_file": app_file,
            "start_time": datetime.now().isoformat(),
            "success": False,
            "error": None,
            "details": {}
        }
        
        try:
            # Initialize the app test
            self.debug_log(f"Initializing AppTest for {app_file}")
            at = AppTest.from_file(app_file)
            
            # Run the app initially
            self.debug_log("Running initial app...")
            at.run()
            
            # Check if app ran without exceptions
            if at.exception:
                test_result["error"] = f"App failed to run: {at.exception}"
                self.debug_log(f"❌ {test_result['error']}")
                return False
            
            self.debug_log("✅ App initialized successfully")
            
            # Run the custom test function
            self.debug_log("Executing test function...")
            test_success = test_function(at)
            
            test_result["success"] = test_success
            test_result["end_time"] = datetime.now().isoformat()
            
            if test_success:
                self.debug_log(f"✅ Test passed: {test_name}")
                self.test_results["passed_tests"] += 1
            else:
                self.debug_log(f"❌ Test failed: {test_name}")
                self.test_results["failed_tests"] += 1
                
        except Exception as e:
            test_result["error"] = str(e)
            test_result["traceback"] = traceback.format_exc()
            self.debug_log(f"❌ Test error: {e}")
            self.test_results["failed_tests"] += 1
            test_success = False
            
        finally:
            self.test_results["tests"].append(test_result)
            self.test_results["total_tests"] += 1
            
        return test_success
    
    def test_button_interaction(self,
                               app_file: str,
                               button_selector: str,
                               expected_result: str,
                               test_name: str = None) -> bool:
        """
        Test button click interactions and verify expected results.
        
        Args:
            app_file: Path to the Streamlit app file
            button_selector: Button selector (key, text, etc.)
            expected_result: Expected result after button click
            test_name: Optional test name
            
        Returns:
            bool: True if test passed
        """
        
        test_name = test_name or f"button_interaction_{button_selector}"
        
        def button_test(at: AppTest) -> bool:
            # Look for the button
            buttons = at.button
            target_button = None
            
            # Try to find button by key or text
            for button in buttons:
                if (hasattr(button, 'key') and button_selector in str(button.key)) or \
                   (hasattr(button, 'label') and button_selector in str(getattr(button, 'label', ''))):
                    target_button = button
                    break
            
            if not target_button:
                self.debug_log(f"❌ Button not found: {button_selector}")
                return False
            
            self.debug_log(f"✅ Found button: {button_selector}")
            
            # Click the button
            target_button.click()
            at.run()
            
            # Check for exceptions after click
            if at.exception:
                self.debug_log(f"❌ Exception after button click: {at.exception}")
                return False
            
            # Check for expected result
            page_content = ""
            
            # Collect content from various elements
            for success_msg in at.success:
                page_content += str(success_msg.value) + " "
            for info_msg in at.info:
                page_content += str(info_msg.value) + " "
            for markdown in at.markdown:
                page_content += str(markdown.value) + " "
            
            if expected_result.lower() in page_content.lower():
                self.debug_log(f"✅ Found expected result: {expected_result}")
                return True
            else:
                self.debug_log(f"❌ Expected result not found: {expected_result}")
                self.debug_log(f"Page content preview: {page_content[:200]}...")
                return False
        
        return self.test_app_component(
            app_file=app_file,
            test_name=test_name,
            test_function=button_test,
            description=f"Test button '{button_selector}' produces expected result '{expected_result}'"
        )
    
    def test_data_loading(self,
                         app_file: str,
                         expected_data_indicators: List[str],
                         test_name: str = "data_loading") -> bool:
        """
        Test that data loads successfully and expected indicators are present.
        
        Args:
            app_file: Path to the Streamlit app file
            expected_data_indicators: List of strings that should be present when data loads
            test_name: Name of the test
            
        Returns:
            bool: True if all data indicators are found
        """
        
        def data_test(at: AppTest) -> bool:
            # Check dataframes
            dataframes = at.dataframe
            if len(dataframes) > 0:
                self.debug_log(f"✅ Found {len(dataframes)} dataframe(s)")
            
            # Collect all text content
            page_content = ""
            
            # Get content from various elements
            for df in dataframes:
                if hasattr(df, 'value') and df.value is not None:
                    page_content += f"dataframe_rows_{len(df.value)} "
            
            for markdown in at.markdown:
                page_content += str(markdown.value) + " "
                
            for metric in at.metric:
                page_content += str(metric.value) + " "
            
            # Check for each expected indicator
            found_indicators = []
            for indicator in expected_data_indicators:
                if indicator.lower() in page_content.lower():
                    found_indicators.append(indicator)
                    self.debug_log(f"✅ Found data indicator: {indicator}")
                else:
                    self.debug_log(f"❌ Missing data indicator: {indicator}")
            
            success = len(found_indicators) == len(expected_data_indicators)
            
            if success:
                self.debug_log(f"✅ All {len(expected_data_indicators)} data indicators found")
            else:
                self.debug_log(f"❌ Found {len(found_indicators)}/{len(expected_data_indicators)} indicators")
                self.debug_log(f"Page content preview: {page_content[:300]}...")
            
            return success
        
        return self.test_app_component(
            app_file=app_file,
            test_name=test_name,
            test_function=data_test,
            description=f"Test data loading with indicators: {expected_data_indicators}"
        )
    
    def test_form_interaction(self,
                             app_file: str,
                             form_inputs: Dict[str, Any],
                             submit_button: str,
                             expected_result: str,
                             test_name: str = "form_interaction") -> bool:
        """
        Test form input and submission.
        
        Args:
            app_file: Path to the Streamlit app file
            form_inputs: Dictionary of input field keys and values
            submit_button: Submit button selector
            expected_result: Expected result after form submission
            test_name: Name of the test
            
        Returns:
            bool: True if form interaction works as expected
        """
        
        def form_test(at: AppTest) -> bool:
            # Fill form inputs
            for input_key, input_value in form_inputs.items():
                # Try different input types
                text_inputs = [inp for inp in at.text_input if hasattr(inp, 'key') and inp.key == input_key]
                selectboxes = [sel for sel in at.selectbox if hasattr(sel, 'key') and sel.key == input_key]
                text_areas = [ta for ta in at.text_area if hasattr(ta, 'key') and ta.key == input_key]
                
                if text_inputs:
                    text_inputs[0].input(input_value)
                    self.debug_log(f"✅ Filled text input {input_key}: {input_value}")
                elif selectboxes:
                    selectboxes[0].select(input_value)
                    self.debug_log(f"✅ Selected {input_key}: {input_value}")
                elif text_areas:
                    text_areas[0].input(input_value)
                    self.debug_log(f"✅ Filled text area {input_key}: {input_value}")
                else:
                    self.debug_log(f"❌ Input field not found: {input_key}")
                    return False
            
            # Submit form
            submit_buttons = [btn for btn in at.button if submit_button in str(getattr(btn, 'key', '')) or 
                             submit_button in str(getattr(btn, 'label', ''))]
            
            if not submit_buttons:
                self.debug_log(f"❌ Submit button not found: {submit_button}")
                return False
            
            submit_buttons[0].click()
            at.run()
            
            # Check for expected result
            page_content = ""
            for success_msg in at.success:
                page_content += str(success_msg.value) + " "
            for info_msg in at.info:
                page_content += str(info_msg.value) + " "
            
            if expected_result.lower() in page_content.lower():
                self.debug_log(f"✅ Form submission successful: {expected_result}")
                return True
            else:
                self.debug_log(f"❌ Expected result not found after form submission")
                return False
        
        return self.test_app_component(
            app_file=app_file,
            test_name=test_name,
            test_function=form_test,
            description=f"Test form with inputs {list(form_inputs.keys())} and submit '{submit_button}'"
        )
    
    def finalize_results(self) -> Dict[str, Any]:
        """
        Finalize test results and determine overall success.
        
        Returns:
            Dict containing complete test results
        """
        
        self.test_results["end_time"] = datetime.now().isoformat()
        self.test_results["success"] = self.test_results["failed_tests"] == 0 and self.test_results["total_tests"] > 0
        
        # Calculate duration
        start_time = datetime.fromisoformat(self.test_results["start_time"])
        end_time = datetime.fromisoformat(self.test_results["end_time"])
        duration = (end_time - start_time).total_seconds()
        self.test_results["duration_seconds"] = duration
        
        return self.test_results
    
    def save_results(self, filename: str = None) -> str:
        """
        Save test results to JSON file.
        
        Args:
            filename: Optional filename, defaults to story-based name
            
        Returns:
            str: Path to saved results file
        """
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"analysis_temp/user_story_test_results_{self.story_number}_{timestamp}.json"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(self.test_results, f, indent=2)
        
        self.debug_log(f"📄 Test results saved to: {filename}")
        return filename
    
    def print_summary(self):
        """Print a formatted summary of test results."""
        
        results = self.finalize_results()
        
        print("\n" + "="*60)
        print(f"🧪 USER STORY TEST SUMMARY")
        print("="*60)
        print(f"📋 Story: {results['story_name']}")
        print(f"🔢 Story Number: {results['story_number']}")
        print(f"⏱️ Duration: {results.get('duration_seconds', 0):.1f} seconds")
        print(f"📊 Tests: {results['total_tests']} total")
        print(f"✅ Passed: {results['passed_tests']}")
        print(f"❌ Failed: {results['failed_tests']}")
        print(f"🎯 Overall Success: {'✅ PASSED' if results['success'] else '❌ FAILED'}")
        
        if results['tests']:
            print(f"\n📋 Individual Test Results:")
            for i, test in enumerate(results['tests'], 1):
                status = "✅ PASSED" if test['success'] else "❌ FAILED"
                print(f"  {i}. {test['name']}: {status}")
                if test.get('description'):
                    print(f"     {test['description']}")
                if test.get('error'):
                    print(f"     Error: {test['error']}")
        
        print("="*60)


def create_story_test_template(story_name: str, story_number: str) -> str:
    """
    Create a test template file for a user story.
    
    Args:
        story_name: Name of the user story
        story_number: Story number (e.g., "009")
        
    Returns:
        str: Path to created template file
    """
    
    template_content = f'''#!/usr/bin/env python3
"""
User Story #{story_number} Test Suite: {story_name}
Generated template for native Streamlit testing
"""

from services.user_story_testing import UserStoryTester

def test_story_{story_number}():
    """Test implementation for Story #{story_number}: {story_name}"""
    
    # Initialize the tester
    tester = UserStoryTester(
        story_name="{story_name}",
        story_number="{story_number}"
    )
    
    # Test 1: Basic app loading
    tester.test_data_loading(
        app_file="app.py",
        expected_data_indicators=["conversations", "data loaded"],  # Customize these
        test_name="basic_app_loading"
    )
    
    # Test 2: Button interaction (customize for your story)
    tester.test_button_interaction(
        app_file="pages/Processor.py",  # Customize app file
        button_selector="your_button_key",  # Customize button
        expected_result="expected success message",  # Customize expected result
        test_name="main_functionality_test"
    )
    
    # Test 3: Form interaction (if applicable)
    # tester.test_form_interaction(
    #     app_file="pages/YourPage.py",
    #     form_inputs={{"field_key": "test_value"}},
    #     submit_button="submit_button_key",
    #     expected_result="form submitted successfully",
    #     test_name="form_submission_test"
    # )
    
    # Add more tests as needed for your specific story requirements
    
    # Finalize and save results
    results = tester.finalize_results()
    tester.save_results()
    tester.print_summary()
    
    return results["success"]

if __name__ == "__main__":
    print("🧪 Running User Story #{story_number} Test Suite")
    print("=" * 50)
    
    success = test_story_{story_number}()
    
    if success:
        print("\\n🎉 All tests passed! Story #{story_number} is ready for completion.")
    else:
        print("\\n❌ Some tests failed. Review the results and fix issues before completing the story.")
'''
    
    filename = f"analysis_temp/test_story_{story_number}.py"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        f.write(template_content)
    
    print(f"📄 Test template created: {filename}")
    return filename

if __name__ == "__main__":
    # Example usage
    print("🧪 User Story Testing Framework")
    print("This framework integrates native Streamlit testing into the user story workflow")
    print("\\nExample usage:")
    print("  from services.user_story_testing import UserStoryTester")
    print("  tester = UserStoryTester('My Story', '009')")
    print("  tester.test_button_interaction('app.py', 'my_button', 'success')")
    print("  tester.print_summary()")