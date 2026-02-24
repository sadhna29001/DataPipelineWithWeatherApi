# Backups Directory

This directory stores timestamped backups of pipeline data.

## Backup Format

Backups are created with timestamp:
- `weather_backup_YYYYMMDD_HHMMSS.csv`

## Creating Backups

Backups can be created programmatically:
```python
from src.load.data_loader import DataLoader

loader = DataLoader()
backup_file = loader.create_backup(df, './backups')
```

## Restoration

To restore from a backup:
```bash
cp backups/weather_backup_20260224_103000.csv data/weather_data.csv
```

## Cleanup

To remove old backups (older than 30 days):
```bash
find backups/ -name "weather_backup_*.csv" -mtime +30 -delete
```
