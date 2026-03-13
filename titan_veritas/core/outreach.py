import os
import httpx
from typing import List, Optional
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText

class GmailService:
    """Gestisce l'autenticazione e le operazioni su Gmail API"""
    SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

    def __init__(self, token_path: str, credentials_path: str):
        self.token_path = token_path
        self.credentials_path = credentials_path
        self.service = self._authenticate()

    def _authenticate(self):
        creds = None
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_path, self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open(self.token_path, 'w') as token:
                token.write(creds.to_json())
        
        return build('gmail', 'v1', credentials=creds)

    def send_localized_email(self, to: str, subject: str, body: str):
        """Invia un'email formattata"""
        message = MIMEText(body)
        message['to'] = to
        message['subject'] = subject
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
        try:
            self.service.users().messages().send(userId='me', body={'raw': raw}).execute()
            return True
        except Exception as e:
            print(f"Errore invio email: {e}")
            return False

class IntelligenceProcessor:
    """Interfaccia Gemini per il processing delle risposte"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key={self.api_key}"

    async def extract_player_info(self, email_text: str) -> str:
        """Estrarre nomi e contatti dalle risposte ricevute"""
        prompt = f"Analizza la seguente risposta email e estrai nomi di calciatori coinvolti, età, club e contatti citati. Rispondi solo in formato JSON:\n\n{email_text}"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}]
        }
        async with httpx.AsyncClient() as client:
            response = await client.post(self.api_url, json=payload)
            return response.json()

class TelegramNotifier:
    """Bot Telegram per notifiche in tempo reale"""
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def send_alert(self, message: str):
        """Invia notifica urgente"""
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown"}
        httpx.post(url, json=payload)

class OutreachCoordinator:
    """Orchestratore della pipeline Active Scouting"""
    def __init__(self, gmail: GmailService, intel: IntelligenceProcessor, tg: TelegramNotifier):
        self.gmail = gmail
        self.intel = intel
        self.tg = tg

    def run_outreach_cycle(self, communities: List[dict]):
        """Ciclo di contatto per una lista di fratellanze"""
        for community in communities:
            body = f"Gentile referente della comunità di {community['name']}, siamo alla ricerca di giovani talenti..."
            success = self.gmail.send_localized_email(community['email'], "Scouting FSGC - San Marino", body)
            if success:
                self.tg.send_alert(f"🚀 Outreach avviato per: *{community['name']}*")
