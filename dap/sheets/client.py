# dap/sheets/client.py

import os
from dataclasses import dataclass

import gspread
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle
from dotenv import load_dotenv


@dataclass(frozen=True)
class SheetsConfig:
    spreadsheet_id: str
    prospects_sheet_name: str = "prospects"
    runs_sheet_name: str = "runs"
    credentials_path: str = ""


def load_sheets_config() -> SheetsConfig:
    load_dotenv()
    
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "").strip()
    prospects_name = os.getenv("GOOGLE_SHEETS_WORKSHEET_NAME", "prospects").strip()
    runs_name = os.getenv("GOOGLE_SHEETS_RUNS_WORKSHEET_NAME", "runs").strip()
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if not spreadsheet_id:
        raise RuntimeError("Missing env var: GOOGLE_SHEETS_SPREADSHEET_ID")
    if not cred_path:
        raise RuntimeError("Missing env var: GOOGLE_APPLICATION_CREDENTIALS")

    return SheetsConfig(
        spreadsheet_id=spreadsheet_id,
        prospects_sheet_name=prospects_name,
        runs_sheet_name=runs_name,
        credentials_path=cred_path,
    )


def _client_from_oauth(credentials_path: str) -> gspread.Client:
    """Use OAuth Desktop App flow"""
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    
    creds = None
    token_path = os.path.join(os.path.dirname(credentials_path), 'token.pickle')
    
    # Load existing token if it exists
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    # If no valid credentials, let user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    return gspread.authorize(creds)


def open_worksheets(cfg: SheetsConfig) -> tuple[gspread.Worksheet, gspread.Worksheet]:
    """
    Returns (prospects_ws, runs_ws) from the configured spreadsheet.
    """
    gc = _client_from_oauth(cfg.credentials_path)
    sh = gc.open_by_key(cfg.spreadsheet_id)

    prospects_ws = sh.worksheet(cfg.prospects_sheet_name)
    runs_ws = sh.worksheet(cfg.runs_sheet_name)
    return prospects_ws, runs_ws