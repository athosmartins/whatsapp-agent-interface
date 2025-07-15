# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

This is a Python Streamlit application. Use these commands:

- **Install dependencies**: `pip install -r requirements.txt`
- **Run the application**: `streamlit run app.py`
- **Run tests**: `python test.py`

## Architecture Overview

This is a WhatsApp conversation processor for real estate lead qualification. The application has three main interfaces:

1. **Dashboard (app.py)**: Main dashboard showing all WhatsApp conversations in a data grid with bulk actions and record navigation
2. **Processor (pages/Processor.py)**: Individual conversation processor for detailed classification and response management
3. **Conversations (pages/Conversations.py)**: Advanced filtering and conversation browsing with integrated Google Sheets data

### Key Components

- **Data Loading**: Uses `loaders/db_loader.py` to download and load SQLite database from Google Drive containing WhatsApp conversations
- **Authentication**: Optional login system via `auth/login_manager.py` (controlled by `LOGIN_ENABLED` flag)
- **Configuration**: `config.py` contains all dropdown options for classification, intentions, payment methods, and preset responses
- **UI Utilities**: `utils/` contains styling and helper functions for parsing conversations and property data
- **Services**: 
  - `services/spreadsheet.py` - Google Sheets integration for data synchronization
  - `services/voxuy_api.py` - WhatsApp message sending via Voxuy API
  - `services/google_drive_loader.py` - Google Drive file operations
  - `services/mega_data_set_loader.py` - Property data integration from mega_data_set

### Data Flow

1. Application loads WhatsApp conversation data from SQLite database (auto-downloaded from Google Drive)
2. Dashboard displays conversations in a grid with phone numbers, names, classifications, and sync status
3. Users can filter conversations using the Conversations page with advanced filtering options
4. Individual conversations can be opened in the Processor page for detailed classification
5. Processor page shows conversation history, property details, and classification forms
6. **Property Integration**: System automatically loads property data via phone number → CPF → mega_data_set mapping
7. Users can send WhatsApp messages directly from the Processor using the Voxuy API integration
8. All modifications are synchronized with Google Sheets and stored in session state
9. Data can be exported to CSV/Excel formats

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

### Message Display System

The conversation history uses a WhatsApp-style chat interface with:
- **Fixed-width containers** (max-width: 400px) for consistent message display
- **Proper text wrapping** that maximizes horizontal space usage
- **Date headers** in Portuguese with context-aware formatting (Hoje, Ontem, etc.)
- **Responsive message bubbles** with different colors for sent/received messages
- **Optimized CSS** using `display: inline-block` for natural text flow

### Property Cross-Reference System

The property listings in the Processor interface include interactive features:
- **Clickable Properties**: Each property has a "🔍" button that shows related conversations. The button is active when related conversations exist, greyed out when none found
- **Related Conversations**: Displays a dataframe with columns: classificacao, display_name, expected_name, phone, status
- **Debug Information**: Comprehensive logging when debug mode is enabled
- **Session State Management**: Uses session state to manage modal visibility and data

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

### Important Notes

- The application expects a specific database schema with a `deepseek_results` table
- Property data is stored as structured text and parsed using utility functions
- The system tracks sync status for Google Sheets and WhatsApp message sending
- Debug mode is available in development for troubleshooting
- Session state is used extensively to maintain data consistency across page navigation
- Google Sheets sync uses `USER_ENTERED` value input option to prevent apostrophe formatting on dates
- **Critical**: CPF numbers with leading zeros must be preserved for accurate property matching

## Development Guidelines

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

### Code Quality
- **Testing in dev**: Always test solutions before marking them as complete. You never need to run a command to test locally, the application will always be up and running on http://localhost:8501/f
- **Testing in prod**: After you push a code to the remote branch, always test all pages on the production url https://urblink-chat.streamlit.app/ - if you find errors, automatically fix, push them, and test agán
- **Documentation**: Update documentation when adding new features or changing architecture
- **Error Recovery**: Implement graceful error handling and recovery mechanisms

### API Integration
- **Voxuy API**: Uses metadata field `mensagem_customizada` for message content
- **Google Sheets API**: Uses `USER_ENTERED` value input option for proper data formatting
