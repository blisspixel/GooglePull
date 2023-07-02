# GooglePull
python script to pull down a Google Drive Folder then delete it
Seemed to work OK for me, but maybe there are issues.  WARNING THIS DELETES FILES.

Prerequisites (in requirements.txt)
Python
google-auth
google-auth-httplib2
google-auth-oauthlib
google-api-python-client
tqdm

Google APIs Client Library for Python: This library is used to interact with the Google Drive API. 
pip install -r requirements.txt

Create a configuration file named config.ini in the same directory as the script. The file should contain one section, [DEFAULT], and one or more of the following parameters:
token_file: The path to a JSON file containing your Google Drive API credentials. Default: token.json.
Example of config.ini file:

Copy code
[DEFAULT]
token_file = token.json
Obtain a Google Drive API token file and store it in the path specified in the config.ini file. The token file should be in JSON format and should contain your API credentials.

Running the Script

Run the script in a terminal or command prompt by typing python script_name.py, replacing script_name.py with the actual name of the script file.
The script will display a list of available sources from your Google Drive, including both files and Team Drives. Enter the number of the source you want to download from.
Next, the script will ask you to enter the destination folder. This should be a valid path on your local system where the downloaded files will be stored.
The script will then download all files from the selected source, verify the downloaded files, and delete them from the source. Progress will be displayed in the terminal.
Troubleshooting
If the script encounters an error, it will display an error message in the terminal and write the same message to a log file. If the error is due to rate limits from the Google Drive API, the script will automatically retry the download after waiting for a few seconds. If the error persists or is due to another cause, you may need to manually intervene to resolve the issue.

Remember to always have a backup of important files and data, and carefully verify the successful completion of the script's operations. Always verify the deletion of files on Google Drive manually, and keep a local backup if those files are crucial.
