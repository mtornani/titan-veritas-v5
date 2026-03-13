import os
import click
from dotenv import load_dotenv
from titan_veritas.core.outreach import GmailService, IntelligenceProcessor, TelegramNotifier, OutreachCoordinator

load_dotenv()

@click.group()
def cli():
    """TITAN VERITAS v5.0 - Active Scouting Outreach Control"""
    pass

@cli.command()
@click.option('--email', required=True, help='Email del destinatario (es. referente Comunità)')
@click.option('--name', required=True, help='Nome della comunità/referente')
def start_outreach(email, name):
    """Avvia un ciclo di contatto verso una comunità specifica"""
    click.echo(f"Inizializzazione Outreach per: {name} ({email})...")
    
    # Inizializzazione servizi (Userà le credenziali dal file .env e JSON)
    try:
        gmail = GmailService(
            token_path=os.getenv("GMAIL_TOKEN_FILE", "token.json"),
            credentials_path=os.getenv("GMAIL_CREDENTIALS_FILE", "credentials.json")
        )
        intel = IntelligenceProcessor(api_key=os.getenv("GEMINI_API_KEY"))
        tg = TelegramNotifier(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            chat_id=os.getenv("TELEGRAM_CHAT_ID")
        )
        
        coordinator = OutreachCoordinator(gmail, intel, tg)
        
        # Test di invio
        communities = [{"name": name, "email": email}]
        coordinator.run_outreach_cycle(communities)
        
        click.secho(f"✔ Ciclo completato per {name}. Notifica Telegram inviata.", fg="green")
    except Exception as e:
        click.secho(f"✘ Errore durante l'outreach: {e}", fg="red")
        click.echo("Nota: Assicurati di avere 'credentials.json' e le chiavi API nel file .env")

if __name__ == "__main__":
    cli()
