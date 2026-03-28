# Telegram Environment Manager Guide
## Admin Control Panel for .env Configuration

---

## 📋 Overview

The **Environment Manager** allows you to view, validate, and edit your `.env` configuration file **directly through Telegram** without needing SSH access or server restarts for viewing.

**Key Features:**
- ✅ Admin-only access (secured to `ADMIN_ID`)
- ✅ View all env variables with sensitive data masked
- ✅ Edit any variable via interactive buttons
- ✅ Automatic backup before editing
- ✅ Value validation for all critical keys
- ✅ User-friendly inline UI with callbacks

---

## 🎮 Commands & Usage

### `/env` - Main Environment Panel

Send `/env` command to open the environment control panel.

**Response Shows:**
```
🔍 Critical Configuration Status:

✅ API_ID: 123***789
✅ API_HASH: abc***xyz
✅ BOT_TOKEN: 123***890
✅ MONGO_URI: mon***017
✅ ADMIN_ID: 123***789
❌ DATABASE_CHANNEL_ID: NOT SET
```

**Three Options:**
1. **📋 Full Config** - View all variables with masked values
2. **✏️ Edit** - Select and modify any variable
3. **📦 Backups** - See list of backup files

---

## ✏️ How to Edit Variables

### Step 1: Open Edit Menu
```
/env → [✏️ Edit]
```

### Step 2: Select Variable
```
Buttons: [API_ID] [API_HASH] [BOT_TOKEN] [SESSION_STRING]
         [MONGO_URI] [DATABASE_CHANNEL_ID] ... etc
```

### Step 3: Enter New Value
```
Bot: "✏️ Edit: API_ID
      Current value: 123***789
      Send new value (reply to this message):"
      
You: Type new value and send
```

### Step 4: Validation & Confirmation
```
✅ API_ID successfully updated!
⚠️ Bot needs restart for changes to take effect.
```

---

## 🔒 Security Features

### 1. **Sensitive Data Masking**

These keys show only first 3 and last 3 characters:
```
API_ID: 123***456
API_HASH: abc***xyz
BOT_TOKEN: 123***890
SESSION_STRING: ABc***XYZ
MONGO_URI: mon***017
DATABASE_CHANNEL_ID: 123***890
ADMIN_ID: 123***789
```

### 2. **Admin-Only Access**

Only users with ADMIN_ID can use `/env`:
```python
filters.user(config.ADMIN_ID)
```

### 3. **Automatic Backups**

Before any change, auto-backup created:
```
.env_backups/
├── env_backup_20260328_143215.env
├── env_backup_20260328_143845.env
└── env_backup_20260328_144310.env
```

### 4. **Value Validation**

All edits are validated before saving:
```
Numeric keys: API_ID, ADMIN_ID, DATABASE_CHANNEL_ID
   → Must be a number

URL keys: SUPPORT_GROUP_LINK, OWNER_PROFILE_LINK
   → Must start with 'http'

API_HASH: → Must be at least 10 chars
BOT_TOKEN: → Must contain ':' (format: ID:TOKEN)
SESSION_STRING: → Must be at least 20 chars
MONGO_URI: → Must start with 'mongodb'
```

---

## 📊 View Modes

### 1. Full Config View
```
📋 Current Environment Variables:

✓ API_HASH = abc***xyz
✓ API_ID = 123***456
✓ ADMIN_ID = 123***789
✓ BOT_TOKEN = 123***890
...
```

Shows ALL variables with masked values.

### 2. Summary View (Default)
```
🔍 Critical Configuration Status:

✅ API_ID: 123***456
✅ MONGO_URI: mon***017
❌ DATABASE_CHANNEL_ID: NOT SET
```

Shows only critical keys with status.

### 3. Backup List
```
📦 Available Backups:

1. env_backup_20260328_143215.env
2. env_backup_20260328_143845.env
3. env_backup_20260328_144310.env
```

---

## 🔄 Supported Variables

| Key | Type | Example | Validation |
|-----|------|---------|-----------|
| API_ID | Numeric | 12345678 | Must be integer |
| API_HASH | String | abcdef123456 | Length >= 10 |
| BOT_TOKEN | String | 123456:ABCdef | Must contain ':' |
| MONGO_URI | String | mongodb://... | Must start with 'mongodb' |
| ADMIN_ID | Numeric | 987654321 | Must be integer |
| DATABASE_CHANNEL_ID | Numeric | -1001234567890 | Negative or positive |
| SESSION_STRING | String | BQDx... | Length >= 20 |
| SUPPORT_GROUP_LINK | URL | https://t.me/group | Must start with 'http' |
| OWNER_PROFILE_LINK | URL | https://t.me/owner | Must start with 'http' |

---

## ⚡ Quick Usage Examples

### Add Missing DATABASE_CHANNEL_ID
```
/env → ✏️ Edit → DATABASE_CHANNEL_ID → -1001234567890
```

### Update MongoDB URI
```
/env → ✏️ Edit → MONGO_URI → mongodb://user:pass@host/db
```

### Change Support Group Link
```
/env → ✏️ Edit → SUPPORT_GROUP_LINK → https://t.me/newsupport
```

### View Full Config
```
/env → 📋 Full Config
```

### Check Backups
```
/env → 📦 Backups
```

---

## ⚠️ Important Notes

### 1. **Bot Restart Required**
After updating any variable, you must restart the bot for changes to take effect:
```bash
# Stop
Ctrl+C

# Restart
python3 bot.py
```

### 2. **Cannot Edit Empty Values**
All fields are required:
```
❌ DATABASE_CHANNEL_ID cannot be empty
```

### 3. **Validation Errors**
Invalid values will be rejected:
```
❌ API_ID must be a number, got: "abc123"
❌ BOT_TOKEN must contain ':', got: "invalidtoken"
```

### 4. **Backups Auto-Cleanup**
Consider cleaning up old backups periodically:
```bash
# List backups
ls .env_backups/

# Delete old ones
rm .env_backups/env_backup_OLDDATE_*.env
```

---

## 🛠️ Troubleshooting

### Issue: "/env command is not working"
**Solution:** Verify your Telegram ID matches ADMIN_ID in .env:
```bash
python3 -c "import config; print(config.ADMIN_ID)"
```

### Issue: "Cannot edit - value rejected"
**Solution:** Check the validation rules above for your key type

### Issue: "Changed value but bot didn't apply"
**Solution:** You MUST restart the bot for changes to apply:
```bash
python3 bot.py
```

### Issue: "No backups showing"
**Solution:** First edit hasn't been made yet. After first edit, backups will appear.

---

## 📝 Code Integration

Already integrated in `bot.py`:
```python
from env_manager import EnvManager

# Initialized
env_manager = EnvManager(".env")

# Command handler
@app.on_message(filters.command("env") & filters.user(config.ADMIN_ID))
async def env_command(client, message):
    text = env_manager.get_env_summary()
    # ... UI buttons ...
```

---

## 🔐 Data Flow

```
User sends /env (ADMIN_ID only)
           ↓
EnvManager.get_env_summary()
           ↓
Load .env file
           ↓
Mask sensitive keys
           ↓
Return formatted string + buttons
           ↓
User clicks "✏️ Edit" button
           ↓
User selects key
           ↓
Bot prompts for new value
           ↓
User sends new value
           ↓
EnvManager.set_value(key, value)
           ↓
Validate value against rules
           ↓
Create backup of current .env
           ↓
Update .env file
           ↓
Update os.environ (current session)
           ↓
Send confirmation to user
           ↓
⚠️ "Bot needs restart to apply"
```

---

## 📚 API Reference (Code Level)

### Get Environment Summary
```python
summary = env_manager.get_env_summary()
# Returns string with critical keys status
```

### Get Full Environment Display
```python
full_display = env_manager.get_env_display()
# Returns string with all variables
```

### Set Value
```python
success, message = env_manager.set_value("API_ID", "123456")
# Returns (bool, str) - success status and message
```

### Get Specific Key
```python
value = env_manager.get_key_value("API_ID")
# Returns string value or None
```

### Get All Keys
```python
keys = env_manager.get_all_keys()
# Returns list of key names
```

### Get Backups List
```python
backups = env_manager.get_backup_list()
# Returns formatted string of backup files
```

---

## 🎯 Best Practices

1. **Check Before Editing** - Use `/env` to view current values first
2. **Validate Formats** - Ensure you know the correct format for each key
3. **Keep Backups** - Don't delete .env_backups folder
4. **Restart After Changes** - Always restart bot to apply changes
5. **Use Summary View** - It shows which keys are missing (marked ❌)
6. **Test Changes** - After restart, test bot functionality

---

## 📞 Support

If you encounter issues:
1. Check ADMIN_ID matches your Telegram ID
2. Review validation rules for the key you're editing
3. Check .env file syntax
4. Restart bot and try again
5. Use `/help` to see available commands

---

**Created by:** Senior Backend Python Developer  
**Module:** env_manager.py  
**Status:** Production-Ready ✓
