
---
Task ID: 8
Agent: Main
Task: Fix VK/TG food handler skipping meals when API returns non-ISO date format

Work Log:
- Analyzed VK food handler (bot/vk/handlers.py:403) - uses `visit.get("date") == today_str` exact string comparison
- Found TG handler (bot/handlers/balance.py:179) has same vulnerability
- Added normalize_date_to_iso() function to bot/utils/formatters.py (handles YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, DD.MM.YYYY, DD/MM.YYYY)
- Updated VK handler import to include normalize_date_to_iso
- Changed VK comparison: visit.get("date") == today_str -> normalize_date_to_iso(visit.get("date", "")) == today_str
- Updated TG handler import to include normalize_date_to_iso
- Changed TG comparison: vdate != today_str -> normalize_date_to_iso(vdate) != today_str
- Syntax check passed for all 3 files
- Docker rebuild successful, 0 errors on startup
- TG+VK both working, food data loading (13+10 visits)
- Commit d966031 pushed to GitHub

Stage Summary:
- Date normalization prevents silently skipping meals when API changes date format
- Both VK and TG food handlers now resilient to different date formats
- No breaking changes to TG bot
