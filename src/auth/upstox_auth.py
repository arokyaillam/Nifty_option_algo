"""
Upstox Authentication Manager
Handles token generation and refresh automatically
"""

import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import logging

import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UpstoxAuthManager:
    """
    Manage Upstox authentication tokens
    
    Features:
    - Store tokens in file
    - Auto-refresh on expiry
    - OAuth2 flow support
    """
    
    TOKEN_FILE = Path("data/upstox_token.json")
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        redirect_uri: str = "http://localhost:8000/callback"
    ):
        """
        Initialize auth manager
        
        Args:
            api_key: Upstox API key
            api_secret: Upstox API secret
            redirect_uri: OAuth redirect URI
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.redirect_uri = redirect_uri
        
        # Ensure data directory exists
        self.TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    def get_login_url(self) -> str:
        """
        Get Upstox login URL for manual authorization
        
        Returns:
            Authorization URL
        """
        base_url = "https://api.upstox.com/v2/login/authorization/dialog"
        params = {
            "client_id": self.api_key,
            "redirect_uri": self.redirect_uri,
            "response_type": "code"
        }
        
        query = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}?{query}"
        
        return url
    
    def generate_token_from_code(self, auth_code: str) -> dict:
        """
        Generate access token from authorization code
        
        Args:
            auth_code: Authorization code from OAuth redirect
            
        Returns:
            Token data with access_token
        """
        url = "https://api.upstox.com/v2/login/authorization/token"
        
        data = {
            "code": auth_code,
            "client_id": self.api_key,
            "client_secret": self.api_secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code"
        }
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = requests.post(url, data=data, headers=headers)
            response.raise_for_status()
            
            token_data = response.json()
            
            # Add expiry time
            token_data["generated_at"] = datetime.now().isoformat()
            token_data["expires_at"] = (
                datetime.now() + timedelta(hours=24)
            ).isoformat()
            
            # Save to file
            self._save_token(token_data)
            
            logger.info("✅ Token generated and saved")
            return token_data
        
        except Exception as e:
            logger.error(f"❌ Token generation failed: {e}")
            raise
    
    def _save_token(self, token_data: dict):
        """Save token to file"""
        with open(self.TOKEN_FILE, "w") as f:
            json.dump(token_data, f, indent=2)
    
    def _load_token(self) -> Optional[dict]:
        """Load token from file"""
        if not self.TOKEN_FILE.exists():
            return None
        
        try:
            with open(self.TOKEN_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"❌ Failed to load token: {e}")
            return None
    
    def is_token_valid(self) -> bool:
        """
        Check if stored token is valid
        
        Returns:
            True if token exists and not expired
        """
        token_data = self._load_token()
        
        if not token_data:
            return False
        
        # Check expiry
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        
        # Consider expired 1 hour before actual expiry
        buffer_time = timedelta(hours=1)
        
        return datetime.now() < (expires_at - buffer_time)
    
    def get_access_token(self) -> Optional[str]:
        """
        Get valid access token
        
        Returns:
            Access token or None if needs refresh
        """
        if not self.is_token_valid():
            logger.warning("⚠️  Token expired or missing")
            return None
        
        token_data = self._load_token()
        return token_data.get("access_token")
    
    def interactive_login(self):
        """
        Interactive login flow for first-time setup
        """
        print("=" * 70)
        print("Upstox Authentication Setup")
        print("=" * 70)
        print()
        
        # Check if token exists and valid
        if self.is_token_valid():
            print("✅ Valid token already exists")
            token = self.get_access_token()
            print(f"   Token: {token[:20]}...")
            print()
            return token
        
        # Generate login URL
        login_url = self.get_login_url()
        
        print("Step 1: Login to Upstox")
        print("-" * 70)
        print(f"Open this URL in browser:")
        print(f"{login_url}")
        print()
        
        print("Step 2: After login, copy the 'code' from redirect URL")
        print("-" * 70)
        print("Example redirect URL:")
        print("http://localhost:8000/callback?code=ABC123")
        print("                                    ^^^^^^")
        print("                              Copy this code")
        print()
        
        auth_code = input("Enter authorization code: ").strip()
        
        if not auth_code:
            print("❌ No code provided")
            return None
        
        print()
        print("Step 3: Generating access token...")
        print("-" * 70)
        
        try:
            token_data = self.generate_token_from_code(auth_code)
            access_token = token_data.get("access_token")
            
            print(f"✅ Success!")
            print(f"   Token: {access_token[:20]}...")
            print(f"   Expires: {token_data['expires_at']}")
            print(f"   Saved to: {self.TOKEN_FILE}")
            print()
            print("=" * 70)
            
            return access_token
        
        except Exception as e:
            print(f"❌ Failed: {e}")
            return None


# ========================
# Testing
# ========================
if __name__ == "__main__":
    """
    Setup Upstox authentication
    Run: uv run python src/auth/upstox_auth.py
    """
    
    print("=" * 70)
    print("Upstox Token Manager")
    print("=" * 70)
    print()
    
    # Get credentials from settings or input
    api_key = input("Enter Upstox API Key: ").strip()
    api_secret = input("Enter Upstox API Secret: ").strip()
    
    if not api_key or not api_secret:
        print("❌ API Key and Secret required")
        print("   Get from: https://upstox.com/developer/apps")
        exit(1)
    
    # Create auth manager
    auth_mgr = UpstoxAuthManager(
        api_key=api_key,
        api_secret=api_secret
    )
    
    # Interactive login
    token = auth_mgr.interactive_login()
    
    if token:
        print("✅ Setup complete! Token saved.")
        print()
        print("Now you can run producers without manual token entry:")
        print("   uv run python scripts/run_producer.py")