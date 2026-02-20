# SensorFor Cloud Downloader

Downloads sensor measurements from the [SensorFor Cloud API](https://www.sensorfor.com)
and stores them in a local SQLite database. Supports scheduled weekly runs and a simple CLI.

## Requirements

- Python 3.11+
- Packages: `requests`, `schedule`

## Setup

```bash
pip install -r requirements.txt
```

## Configuration

Edit the **CONFIGURATION** section near the top of `sensorfor_downloader.py`:

| Setting | Description |
|---|---|
| `DEVICES` | List of devices; each needs `device_id`, `alias`, and `field_names` |
| `LINES` | Records to fetch per download (max 960) |
| `ZOOM` | Time resolution: `1` = highest, `20` = lowest |
| `SCHEDULE_DAY` | Day of week for the weekly download (`monday` … `sunday`) |
| `SCHEDULE_TIME` | Time of day in 24 h format, e.g. `"02:00"` |
| `DB_PATH` | SQLite file path (default: `sensorfor.db`) |
| `LOG_PATH` | Log file path (default: `sensorfor.log`) |

### Device configuration example

```python
DEVICES = [
    {
        "device_id": "12345",        # printed on the device label
        "alias": "office_sensor",    # short name used in --query
        "field_names": [             # names for measurement columns (fields 7+)
            "temperature",
            "humidity",
            "co2",
        ],
    },
]
```

If `field_names` is shorter than the number of fields in the API response,
the remaining columns are named automatically as `field_0`, `field_1`, etc.

## Usage

### Run scheduler (weekly)

Runs in the foreground and triggers a download every week on the configured day/time.

```bash
python sensorfor_downloader.py
```

Press **Ctrl+C** to stop.

### Immediate download

Triggers one download for all devices and exits. Useful for testing or cron jobs.

```bash
python sensorfor_downloader.py --run-now
```

### Show download history

Prints the last 20 entries from `download_log`.

```bash
python sensorfor_downloader.py --status
```

Example output:

```
 ID  Device                  Downloaded at        Fetched     New  Status
──────────────────────────────────────────────────────────────────────────────
  1  office_sensor           2024-09-18 02:00:05      960     823  ok
  2  warehouse_sensor        2024-09-18 02:00:07      960     960  ok
```

### Query last records for a device

Prints the last 10 measurements for the given device alias.

```bash
python sensorfor_downloader.py --query office_sensor
```

Example output:

```
Last 10 measurements for 'office_sensor':

  2024-09-18T15:05:10
    temperature              23.4
    humidity                 51
    co2                      412
  2024-09-18T15:00:10
    ...
```

## Database schema

| Table | Purpose |
|---|---|
| `devices` | One row per registered device |
| `measurements` | One row per record; `(device_id, timestamp)` is unique |
| `measurement_fields` | One row per field per measurement (EAV layout) |
| `download_log` | Audit log of every download run |

You can query the database directly with any SQLite client, e.g.:

```bash
sqlite3 sensorfor.db "SELECT * FROM measurements ORDER BY timestamp DESC LIMIT 5;"
```

## Running as a scheduled service

### Linux / macOS — systemd user service

Create `~/.config/systemd/user/sensorfor.service`:

```ini
[Unit]
Description=SensorFor Cloud Downloader

[Service]
ExecStart=/usr/bin/python3 /path/to/sensorfor_downloader.py
Restart=on-failure

[Install]
WantedBy=default.target
```

```bash
systemctl --user enable --now sensorfor.service
```

### Windows — Task Scheduler

1. Open **Task Scheduler** → **Create Basic Task**.
2. Set the trigger to the desired schedule.
3. Action: `python.exe`, arguments: `C:\path\to\sensorfor_downloader.py --run-now`.

Or use the built-in scheduler by running the script without `--run-now` and keeping
the process alive (e.g. via a shortcut in the Startup folder).
