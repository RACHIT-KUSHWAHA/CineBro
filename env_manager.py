"""
Environment Manager Module for CineBro Bot
Allows admin to view and edit .env file directly through Telegram
with validation, masking of sensitive data, and change tracking.

Author: Senior Backend Developer
"""

import os
import json
import re
from typing import Dict, Tuple, Optional
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class EnvManager:
    """Manage .env file with validation and security."""
    
    # Sensitive keys that should be masked in display
    SENSITIVE_KEYS = {
        'API_HASH', 'BOT_TOKEN', 'SESSION_STRING', 'MONGO_URI',
        'DATABASE_CHANNEL_ID', 'ADMIN_ID'  # These reveal IDs
    }
    
    # Keys that require validation (numeric)
    NUMERIC_KEYS = {
        'API_ID', 'ADMIN_ID', 'DATABASE_CHANNEL_ID',
        'STORAGE_CHANNEL', 'BACKUP_CHANNEL', 'LOG_CHANNEL_ID',
        'SUPPORT_GROUP_ID'
    }
    
    # Keys that are URLs
    URL_KEYS = {'SUPPORT_GROUP_LINK', 'OWNER_PROFILE_LINK'}
    
    def __init__(self, env_path: str = ".env"):
        """
        Initialize EnvManager.
        
        Args:
            env_path: Path to .env file (default: .env in current dir)
        """
        self.env_path = Path(env_path)
        self.backup_dir = Path(".env_backups")
        self.backup_dir.mkdir(exist_ok=True)
        
    def _load_env(self) -> Dict[str, str]:
        """Load .env file as dictionary."""
        env_dict = {}
        try:
            if not self.env_path.exists():
                logger.warning(f".env file not found at {self.env_path}")
                return env_dict
            
            with open(self.env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith('#'):
                        continue
                    # Parse KEY=VALUE
                    if '=' in line:
                        key, value = line.split('=', 1)
                        env_dict[key.strip()] = value.strip()
            
            return env_dict
        except Exception as e:
            logger.error(f"Error loading .env: {e}")
            return env_dict
    
    def _mask_value(self, key: str, value: str) -> str:
        """
        Mask sensitive values for display.
        
        Args:
            key: Variable name
            value: Variable value
            
        Returns:
            Masked or original value
        """
        if key in self.SENSITIVE_KEYS:
            if not value:
                return "[EMPTY]"
            if len(value) <= 4:
                return "***"
            # Show first 3 and last 3 characters
            return f"{value[:3]}***{value[-3:]}"
        return value
    
    def get_env_display(self) -> str:
        """
        Get formatted .env file for display in Telegram.
        
        Returns:
            Formatted string with all env variables (sensitive data masked)
        """
        env_dict = self._load_env()
        
        if not env_dict:
            return "❌ .env file is empty or not found"
        
        lines = ["📋 <b>Current Environment Variables:</b>\n"]
        
        for key in sorted(env_dict.keys()):
            value = env_dict[key]
            masked_value = self._mask_value(key, value)
            status = "✓" if value else "⚠️"
            lines.append(f"{status} <code>{key}</code> = <code>{masked_value}</code>")
        
        return "\n".join(lines)
    
    def get_env_summary(self) -> str:
        """Get brief summary of critical env variables."""
        env_dict = self._load_env()
        
        critical_keys = [
            'API_ID', 'API_HASH', 'BOT_TOKEN', 'MONGO_URI',
            'ADMIN_ID', 'DATABASE_CHANNEL_ID'
        ]
        
        lines = ["🔍 <b>Critical Configuration Status:</b>\n"]
        
        for key in critical_keys:
            value = env_dict.get(key, "")
            if value:
                masked = self._mask_value(key, value)
                lines.append(f"✅ {key}: {masked}")
            else:
                lines.append(f"❌ {key}: <b>NOT SET</b>")
        
        return "\n".join(lines)
    
    def validate_value(self, key: str, value: str) -> Tuple[bool, str]:
        """
        Validate new value before setting.
        
        Args:
            key: Variable name
            value: New value
            
        Returns:
            (is_valid, error_message or success_message)
        """
        # Check if empty
        if not value or not value.strip():
            return False, f"❌ {key} cannot be empty"
        
        value = value.strip()
        
        # Validate numeric keys
        if key in self.NUMERIC_KEYS:
            if not value.lstrip('-').isdigit():
                return False, f"❌ {key} must be a number, got: {value}"
        
        # Validate URLs
        if key in self.URL_KEYS:
            if not value.startswith('http'):
                return False, f"❌ {key} must be a valid URL, got: {value}"
        
        # Validate API_HASH length
        if key == 'API_HASH' and len(value) < 10:
            return False, f"❌ API_HASH seems too short (got {len(value)} chars)"
        
        # Validate BOT_TOKEN format
        if key == 'BOT_TOKEN' and ':' not in value:
            return False, f"❌ BOT_TOKEN must contain ':' (format: ID:TOKEN)"
        
        # Validate SESSION_STRING (long string)
        if key == 'SESSION_STRING' and len(value) < 20:
            return False, f"❌ SESSION_STRING seems too short (got {len(value)} chars)"
        
        # Validate MONGO_URI
        if key == 'MONGO_URI' and not value.startswith('mongodb'):
            return False, f"❌ MONGO_URI must start with 'mongodb'"
        
        return True, f"✅ {key} validation passed"
    
    def _create_backup(self) -> bool:
        """
        Create backup of current .env file before editing.
        
        Returns:
            True if backup created successfully
        """
        try:
            if not self.env_path.exists():
                return True  # No backup needed if file doesn't exist
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"env_backup_{timestamp}.env"
            
            with open(self.env_path, 'r') as f:
                content = f.read()
            
            with open(backup_path, 'w') as f:
                f.write(content)
            
            logger.info(f"✓ Backup created: {backup_path}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to create backup: {e}")
            return False
    
    def set_value(self, key: str, value: str) -> Tuple[bool, str]:
        """
        Set environment variable in .env file.
        
        Args:
            key: Variable name
            value: New value
            
        Returns:
            (success, message)
        """
        # Validate value
        is_valid, validation_msg = self.validate_value(key, value)
        if not is_valid:
            return False, validation_msg
        
        try:
            # Create backup before modifying
            self._create_backup()
            
            # Load current .env
            env_dict = self._load_env()
            
            # Update or add new key
            env_dict[key] = value.strip()
            
            # Write back to file
            with open(self.env_path, 'w') as f:
                for k, v in env_dict.items():
                    f.write(f"{k}={v}\n")
            
            # Update os.environ as well (for current session)
            os.environ[key] = value.strip()
            
            logger.info(f"✓ Updated {key} in .env file")
            return True, f"✅ <b>{key}</b> updated successfully!\n\n" \
                         f"⚠️ Bot needs restart for changes to take effect."
        
        except Exception as e:
            logger.error(f"✗ Error setting {key}: {e}")
            return False, f"❌ Error updating {key}: {e}"
    
    def get_key_value(self, key: str) -> Optional[str]:
        """Get specific env variable value."""
        env_dict = self._load_env()
        return env_dict.get(key)
    
    def get_all_keys(self) -> list:
        """Get list of all env keys."""
        env_dict = self._load_env()
        return sorted(env_dict.keys())
    
    def get_editable_keys(self) -> list:
        """Get list of keys that are safe to edit via Telegram."""
        # Return all keys (user should be careful with sensitive ones)
        return self.get_all_keys()
    
    def get_backup_list(self) -> str:
        """Get list of available backups."""
        backups = sorted(self.backup_dir.glob("env_backup_*.env"))
        
        if not backups:
            return "📦 No backups found"
        
        lines = ["📦 <b>Available Backups:</b>\n"]
        for i, backup in enumerate(backups[-10:], 1):  # Show last 10
            lines.append(f"{i}. {backup.name}")
        
        return "\n".join(lines)


# ============================================================================
# INTEGRATION EXAMPLE (Use in your bot.py)
# ============================================================================

r"""
In your bot.py, add imports at top:

from env_manager import EnvManager

# Create instance
env_manager = EnvManager(".env")

# Then add these command handlers:

@app.on_message(filters.command("env") & filters.user(config.ADMIN_ID))
async def env_command(client: Client, message: Message):
    '''Admin command to view environment variables.'''
    text = env_manager.get_env_summary()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Full Config", callback_data="env_full"),
         InlineKeyboardButton("✏️ Edit", callback_data="env_edit_menu")],
        [InlineKeyboardButton("📦 Backups", callback_data="env_backups")]
    ])
    await message.reply_text(text, reply_markup=keyboard)


@app.on_callback_query(filters.regex(r"^env_") & filters.user(config.ADMIN_ID))
async def env_callback(client: Client, call: CallbackQuery):
    '''Handle env-related callbacks.'''
    # See bot.py for complete implementation
    pass


@app.on_message(filters.reply & filters.user(config.ADMIN_ID) & ~filters.command())
async def handle_env_edit(client: Client, message: Message):
    '''Handle replies to env edit prompts.'''
    if not message.reply_to_message:
        return
    
    reply_text = message.reply_to_message.text or ""
    
    # Check if this is an env edit prompt
    if "Edit:" not in reply_text:
        return
    
    # Extract key using regex with raw string
    import re
    match = re.search(r"Edit: (\w+)", reply_text)
    if not match:
        return
    
    key = match.group(1)
    new_value = message.text.strip()
    
    # Update the value
    success, result_msg = env_manager.set_value(key, new_value)
    
    if success:
        await message.reply_text(result_msg + "\\n\\nNote: Restart bot to apply changes")
    else:
        await message.reply_text(result_msg)
"""
