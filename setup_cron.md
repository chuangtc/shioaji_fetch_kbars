# Cron Job Setup for Stock Kbars Fetcher

## Setup Instructions

### 1. Make the shell script executable

```bash
chmod +x /home/jason/workspace/shioaji_fetch_kbars/run_fetch_kbars.sh
```

### 2. Edit your crontab

```bash
crontab -e
```

### 3. Add the cron job entry

Add this line to run every weekday (Monday-Friday) at 17:00 Taipei time:

```cron
# Fetch stock kbars every weekday at 17:00 Taipei time
0 17 * * 1-5 cd /home/jason/workspace/shioaji_fetch_kbars && TZ='Asia/Taipei' /home/jason/workspace/shioaji_fetch_kbars/run_fetch_kbars.sh
```

**Explanation:**

- `0 17 * * 1-5`: Run at 17:00 (5:00 PM) on Monday through Friday
  - `0`: Minute (0)
  - `17`: Hour (17 = 5 PM)
  - `*`: Day of month (any)
  - `*`: Month (any)
  - `1-5`: Day of week (Monday=1 through Friday=5)
- `TZ='Asia/Taipei'`: Set timezone to Taipei
- The script will automatically fetch today's kbar data

### 4. Verify cron job is installed

```bash
crontab -l
```

You should see your cron job listed.
