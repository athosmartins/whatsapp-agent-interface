# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 🔄 USER STORY WORKFLOW - MANDATORY PROCESS

**CRITICAL: This workflow MUST be followed for ALL code changes and feature requests.**

### 1. Story Definition Phase
When a user requests code changes or new features, ALWAYS:

1. **Read USER_STORIES.md first** to understand previously solved problems and prevent regression
2. **Create a story plan** with:
   - Name of the user story
   - What is the problem
   - Why is the problem important
   - Success criteria (what defines completion)
3. **Ask clarifying questions** if the story isn't clear
4. **Confirm the story** with the user before proceeding

### 2. Story Commitment Phase
**IMMEDIATELY after user approval of the story:**

1. **Write the approved story to USER_STORIES.md** as "WIP" (Work In Progress)
2. **Include the complete story definition** with:
   - Story name, problem, importance
   - Success criteria agreed upon
   - Tasks to accomplish
   - Tests needed to verify completion
3. **This serves as our contract** - both Claude and user can reference this commitment

### 3. Development Phase
During development:

1. **Monitor for scope creep** - if the user deviates from the original story, warn them and confirm if it's intentional
2. **If success criteria changes during development:**
   - **EXPLICITLY tell the user** that criteria are changing from the original commitment
   - **Ask permission** to modify the story criteria in USER_STORIES.md
   - **Update the WIP story** only after user approval
3. **Reference previous stories** to ensure no regression of solved problems
4. **After completing each task:**
   - **Update USER_STORIES.md** to mark task as completed in the WIP story section
   - **IMMEDIATELY ask user to test the completed task** before proceeding to the next task
   - **WAIT for user confirmation** that the task works as expected
   - **DO NOT proceed to the next task** until user has tested and approved
5. **Focus on completing the agreed story** without deviation

### 4. Testing & Verification Phase (MANDATORY)
**BEFORE claiming story completion, check against original WIP story criteria:**

1. **Review the WIP story in USER_STORIES.md** to verify all success criteria are met
2. **Playwright Testing**: Test all functionality with browser automation
3. **Debug Log Monitoring**: Monitor `/tmp/mega_data_debug.log` for:
   - Data flow verification
   - Session state consistency 
   - Expensive operation detection
   - Filter synchronization validation
4. **Performance Analysis**: Verify no regressions or unnecessary operations
5. **Fix Issues Found**: Address any problems discovered during testing
6. **Only proceed to completion after thorough testing confirms expected behavior**

### 5. Completion Phase
When testing confirms the story works correctly:

1. **WAIT FOR USER TESTING AND APPROVAL** - Do NOT mark story as complete until:
   - User has thoroughly tested the implementation
   - User has confirmed all requirements are met
   - User has explicitly approved the story completion
2. **ONLY AFTER USER APPROVAL, update USER_STORIES.md** to change status from "WIP" to "COMPLETED"
3. **Add completion summary** with:
   - Tasks accomplished
   - Main problems encountered
   - Lessons learned
   - Tests that confirmed completion
   - Commit message for terminal use
4. **Assign sequential story number**

### 6. Regression Prevention
- **ALWAYS review USER_STORIES.md** before starting any new story
- **Cross-reference** current changes against previously solved problems
- **Ensure** new code doesn't reintroduce old issues

## Common Commands

This is a Python Streamlit application. Use these commands:

- **Install dependencies**: `pip install -r requirements.txt`
- **Run the application**: `streamlit run app.py`
- **Run tests**: `python test.py`

## CRITICAL DATABASE RULES

**🚨 NEVER USE LOCAL DATABASE FILES 🚨**

- **ALWAYS download database from Google Drive folder ID**: `1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw`
- **NEVER use local files**: No local database files are allowed in development or production
- **ALWAYS force fresh download**: Even if a local file exists, it must be replaced with fresh download
- **Database source is**: Google Drive folder, not local storage
- **This applies to**: Development, production, testing - ALL environments

## Architecture Overview

This is a WhatsApp conversation processor for real estate lead qualification. The application has three main interfaces:

1. **Dashboard (app.py)**: Main dashboard showing all WhatsApp conversations in a data grid with bulk actions and record navigation
2. **Processor (pages/Processor.py)**: Individual conversation processor for detailed classification and response management
3. **Conversations (pages/Conversations.py)**: Advanced filtering and conversation browsing with integrated Google Sheets data

### Key Components

- **Data Loading**: Uses `loaders/db_loader.py` to download and load SQLite database from Google Drive containing WhatsApp conversations
- **CRITICAL DATABASE RULE**: NEVER use local database files. ALWAYS download fresh from Google Drive folder ID `1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw`. Local database files are FORBIDDEN in both development and production.
- **Authentication**: Optional login system via `auth/login_manager.py` (controlled by `LOGIN_ENABLED` flag)
- **Configuration**: `config.py` contains all dropdown options for classification, intentions, payment methods, and preset responses
- **UI Utilities**: `utils/` contains styling and helper functions for parsing conversations and property data
- **Services**: 
  - `services/spreadsheet.py` - Google Sheets integration for data synchronization
  - `services/voxuy_api.py` - WhatsApp message sending via Voxuy API
  - `services/google_drive_loader.py` - Google Drive file operations
  - `services/mega_data_set_loader.py` - Property data integration from mega_data_set

### Data Flow

1. Application loads WhatsApp conversation data from SQLite database (ALWAYS auto-downloaded fresh from Google Drive folder ID `1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw`)
2. Dashboard displays conversations in a grid with phone numbers, names, classifications, and sync status
3. Users can filter conversations using the Conversations page with advanced filtering options
4. Individual conversations can be opened in the Processor page for detailed classification
5. Processor page shows conversation history, property details, and classification forms
6. **Property Integration**: System automatically loads property data via phone number → CPF → mega_data_set mapping
7. Users can send WhatsApp messages directly from the Processor using the Voxuy API integration
8. All modifications are synchronized with Google Sheets and stored in session state
9. **Google Sheets Integration**: Data is synced to the controle_voxuy spreadsheet (ID: `1vJItZ03PiZ4Y3HSwBnK_AUCOiQK32OkynMCd-1exU9k`, tab: `report`)
10. **Auto-Row Creation**: When a phone number is not found in the spreadsheet, a new row is automatically created with proper defaults
11. Data can be exported to CSV/Excel formats

### Key Features

- **Real-time UI updates**: Changes are immediately reflected in the interface using Streamlit's reactive model
- **Session state management**: Tracks original AI values vs. user modifications
- **Preset responses**: Common responses are available as templates with send functionality
- **Property parsing**: Extracts and displays property information from structured data
- **Authentication**: Optional login system for production use
- **Mobile-friendly**: Interface designed for processing WhatsApp conversations
- **Google Sheets Integration**: Bi-directional sync with Google Sheets for data persistence
- **WhatsApp Messaging**: Direct message sending via Voxuy API integration
- **Advanced Filtering**: Comprehensive filtering system with alphabetical sorting
- **Conversation Navigation**: Seamless navigation between different conversation views
- **Property Cross-Reference**: Clickable property listings that show all conversations related to the same property
- **Mega Data Set Integration**: Automatic property lookup using phone number → CPF → property mapping

### Mega Data Set Integration

The system includes comprehensive property data integration via the mega_data_set:

- **Data Source**: Always uses the newest file from Google Drive folder ID `1yFhxSOAf9UdarCekCKCg1UqKl3MArZAp`
- **Data Volume**: Contains 350k+ property records with complete cadastral information
- **Mapping Flow**: Phone number → CPF (via Google Sheets) → Properties (via mega_data_set)
- **CPF Matching**: Preserves leading zeros in CPF numbers for accurate matching (e.g., `00946789606`)
- **Property Display**: Shows detailed property information including cadastral index, areas, and property types
- **Fallback System**: Uses sample data when real mega_data_set is not accessible
- **Caching**: Implements intelligent caching to avoid repeated file downloads
- **Debug Support**: Comprehensive debugging shows every step of the mapping process

## Data pipeline
1. Download **Parquet** from Drive folder `1yFh…`; fallback DuckDB → JSON.gz → CSV.  
2. `loaders/db_loader` pulls WhatsApp `.db`.  
3. Pages call `get_dataframe()` / `get_properties()` (both `st.cache_data(ttl=3600, max_entries=1)`).  
4. Column projection + push‑down keep RSS < 1 GB and WebSocket < 200 MB.

## Resource limits (Community Cloud)
- CPU ≤ 2 cores, RAM ≤ 2.7 GB, WebSocket message ≤ 200 MB :contentReference[oaicite:5]{index=5}  

### Important Notes

- The application expects a specific database schema with a `deepseek_results` table
- Property data is stored as structured text and parsed using utility functions
- The system tracks sync status for Google Sheets and WhatsApp message sending
- Debug mode is available in development for troubleshooting
- Session state is used extensively to maintain data consistency across page navigation
- Google Sheets sync uses `USER_ENTERED` value input option to prevent apostrophe formatting on dates
- **Critical**: CPF numbers with leading zeros must be preserved for accurate property matching
- **controle_voxuy**: Always refers to spreadsheet ID `1vJItZ03PiZ4Y3HSwBnK_AUCOiQK32OkynMCd-1exU9k`, tab `report`
- **Phone Number Format**: New rows use format `+55` + area code + `9` + 8 digits (e.g., `+5531991234567`)
- **Auto-Row Creation**: Missing phone numbers automatically create new rows with proper defaults

## Development Guidelines

### 🚨 DATABASE LOADING - CRITICAL RULE 🚨
- **NEVER use local database files** - Always download fresh from Google Drive folder ID `1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw`
- **Delete any existing local database files** before downloading fresh copy
- **This rule applies to ALL environments**: development, production, testing
- **No exceptions**: Even if local file exists, it must be replaced with Google Drive version

### File Organization
- **Temporary/Analysis/Debug/Test Files**: Store all debug, analysis, and temporary files that are not needed for production in the `analysis_temp/` folder, never create them on the root or any other folder.
- **Production Files**: Keep only essential files in the main directory structure
- **Testing**: Always test solutions thoroughly before confirming they work

### Security Best Practices
- **Secrets Management**: Handle all API keys, tokens, and sensitive data securely
- **Environment Variables**: Use environment variables or separate config files for secrets
- **Git Safety**: Ensure no secrets are committed to public repositories
- **API Keys**: Store API tokens in separate files that are added to `.gitignore`

### Architecture Principles
- **Simplicity**: Keep the architecture as simple as possible while maintaining functionality
- **Performance**: Ensure all frontend actions are smooth and agile for the user
- **Responsiveness**: Prioritize user experience and interface responsiveness
- **Maintainability**: Write clean, readable code with clear separation of concerns

### Debug and Logging
- **Debug Mode**: For every new feature, implement comprehensive debug logging when debug mode is enabled
- **Error Handling**: Provide detailed error messages and stack traces in debug mode
- **Performance Monitoring**: Log performance metrics and bottlenecks when debugging
- **User Feedback**: Ensure users get clear feedback on all actions, especially errors
- **Capabilites**: if i ask you a task that you can't perform, please search for an MCP that can grant you those capabilites. Then, download, install them and proceed to do the task that you previously couldn't

### Code Quality
- **Testing in dev**: Always test solutions before marking them as complete. 
- **Git Commits**: NEVER commit or push code to GitHub without explicit user permission. Always ask first.
- **Documentation**: Update documentation when adding new features or changing architecture
- **Error Recovery**: Implement graceful error handling and recovery mechanisms

### API Integration
- **Voxuy API**: Uses metadata field `mensagem_customizada` for message content
- **Google Sheets API**: Uses `USER_ENTERED` value input option for proper data formatting

### Streamlit Widget State Management - CRITICAL BUG FIX
- **Filter Value Disappearing Bug**: NEVER mix widget state (`key` parameter) with custom session state tracking
- **Root Cause**: When using `st.multiselect` with both `key` and `default` parameters, updating session state after widget creation causes race conditions
- **Solution**: Always use widget state as the single source of truth via the `key` parameter
- **Implementation Pattern**: 
  ```python
  # CORRECT: Use widget state as source of truth
  if widget_key in st.session_state:
      current_selections = st.session_state[widget_key]
  else:
      current_selections = st.session_state.filter_state.get(filter_key, [])
  
  selected = st.multiselect(
      label,
      options=options,
      default=current_selections,
      key=widget_key
  )
  # Sync back to persistent state
  st.session_state.filter_state[filter_key] = selected
  ```
- **NEVER DO**: Don't update session state after widget creation without checking widget state first
- **Apply to**: All multiselect, selectbox, checkbox, and other widgets that need persistence
- **Testing**: Always test filter persistence by selecting value, changing page, and returning to verify value persists
