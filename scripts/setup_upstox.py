"""
One-time Upstox Setup
Run this once to configure authentication
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.auth.upstox_auth import UpstoxAuthManager

if __name__ == "__main__":
    print("=" * 70)
    print("Upstox Setup - One Time Configuration")
    print("=" * 70)
    print()
    
    api_key = input("Upstox API Key: ").strip()
    api_secret = input("Upstox API Secret: ").strip()
    
    auth_mgr = UpstoxAuthManager(api_key, api_secret)
    auth_mgr.interactive_login()