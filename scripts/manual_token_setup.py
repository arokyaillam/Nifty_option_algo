"""
Manual Token Setup (Bypass Firewall Issues)
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

# Create data directory
data_dir = Path("data")
data_dir.mkdir(exist_ok=True)

print("=" * 70)
print("Manual Token Setup")
print("=" * 70)
print()

print("Get your token manually:")
print("1. Go to: https://account.upstox.com/developer/apps")
print("2. Generate Access Token")
print("3. Copy the token")
print()

access_token = input("Paste access token: ").strip()

if not access_token:
    print("❌ No token provided")
    exit(1)

# Create token data
token_data = {
    "access_token": access_token,
    "token_type": "Bearer",
    "expires_in": 86400,
    "generated_at": datetime.now().isoformat(),
    "expires_at": (datetime.now() + timedelta(hours=24)).isoformat()
}

# Save
token_file = data_dir / "upstox_token.json"
with open(token_file, "w") as f:
    json.dump(token_data, f, indent=2)

print()
print("✅ Token saved!")
print(f"   File: {token_file}")
print(f"   Token: {access_token[:30]}...")
print()
print("Now you can run:")
print("   uv run python scripts/run_producer.py")
print()