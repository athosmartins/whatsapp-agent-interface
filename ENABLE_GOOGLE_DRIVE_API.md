# How to Enable Google Drive API for Mega Data Set Integration

## Issue
The system cannot download the mega_data_set file because the Google Drive API is not enabled for your service account project.

## Error Message
```
Google Drive API has not been used in project 656467585915 before or it is disabled. 
Enable it by visiting https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=656467585915
```

## Solution Steps

### Option 1: Enable Google Drive API (Recommended)

1. **Go to Google Cloud Console**
   - Visit: https://console.developers.google.com/apis/api/drive.googleapis.com/overview?project=656467585915
   - Or go to: https://console.cloud.google.com/

2. **Select Your Project**
   - Make sure project `656467585915` is selected
   - This is the project associated with your `credentials.json` file

3. **Enable Google Drive API**
   - Search for "Google Drive API" 
   - Click "Enable"
   - Wait a few minutes for the API to be fully enabled

4. **Test the Integration**
   - Run the Streamlit app with Debug Mode enabled
   - The system should now be able to download the real mega_data_set (350k+ rows)

### Option 2: Manual File Download (Alternative)

If you cannot enable the API, you can manually provide the file:

1. **Download the file manually**
   - Go to Google Drive folder: `1yFhxSOAf9UdarCekCKCg1UqKl3MArZAp`
   - Download the newest CSV file

2. **Place the file in one of these locations:**
   - `/tmp/mega_data_set.csv`
   - `/tmp/mega_data_set_latest.csv`
   - `mega_data_set.csv` (in project root)

3. **Or set the manual path**
   - Edit `services/mega_data_set_loader.py`
   - Set: `MANUAL_MEGA_DATA_SET_PATH = "/path/to/your/file.csv"`

## Verification

After enabling the API or providing the file manually:

1. **Enable Debug Mode** in the Processor interface
2. **Select any conversation record**
3. **Check the debug output** - it should show:
   - Real row count (350k+ instead of 1400)
   - Successful phone → CPF → property mapping
   - Actual property matches for records with valid CPF mappings

## Current Status

✅ **CPF matching fixed** - Leading zeros are preserved  
✅ **Debug system working** - Shows detailed mapping process  
⚠️ **Google Drive API disabled** - Needs to be enabled  
⚠️ **Using sample data** - 1400 rows instead of 350k+  

After enabling the API, the system will automatically download and use the real mega_data_set file.