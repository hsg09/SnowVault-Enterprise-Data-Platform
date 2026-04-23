# Disaster Recovery Runbook

## Quick Reference

| Metric | Target | Current Config |
|---|---|---|
| **RPO** (Recovery Point Objective) | < 1 hour | Hourly failover group replication |
| **RTO** (Recovery Time Objective) | < 15 minutes | Client Redirect + automated promotion |
| **Data Loss Tolerance** | Zero (for Strategy 2) | Kafka offset tracking + Snowpipe replay |

---

## Failover Procedure

### Step 1: Detect Outage

- **Dagster**: `replication_lag_sensor` detects lag > 2 hours → halts downstream assets
- **Snowflake**: `AUDIT.CONTROL.REPLICATION_HEALTH` view shows SLA_STATUS = 'CRITICAL'
- **External**: PagerDuty/Slack alert from monitoring stack

### Step 2: Choose Strategy

| If... | Then use... |
|---|---|
| Brief outage, dashboards needed immediately | **Strategy 1: Reads Before Writes** |
| Extended outage, zero data loss required | **Strategy 2: Writes Before Reads** |
| Critical outage, immediate full recovery | **Strategy 3: Simultaneous Failover** |

### Step 3: Execute (Strategy 2 — Recommended Default)

```sql
-- 1. ON SECONDARY ACCOUNT: Promote failover group
USE ROLE ACCOUNTADMIN;
ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;

-- 2. Verify promotion
SELECT SYSTEM$GET_FAILOVER_GROUP_STATUS('FG_DATA_PLATFORM');

-- 3. Run reconciliation ETL
-- Dagster CDC sensor will auto-detect pending stream data
-- Kafka connector resumes from last committed offset token

-- 4. Redirect clients
ALTER CONNECTION DATA_PLATFORM_CONNECTION
    PRIMARY ACCOUNT <secondary_account>;

-- 5. Verify client connectivity
-- Check BI dashboards, API connections, dbt debug
```

### Step 4: Failback (After Primary Recovery)

```sql
-- 1. ON ORIGINAL PRIMARY: Refresh failover group
ALTER FAILOVER GROUP FG_DATA_PLATFORM REFRESH;

-- 2. Wait for refresh to complete (monitor REPLICATION_HEALTH view)

-- 3. Promote back to original primary
ALTER FAILOVER GROUP FG_DATA_PLATFORM PRIMARY;

-- 4. Revert client redirect
ALTER CONNECTION DATA_PLATFORM_CONNECTION
    PRIMARY ACCOUNT <original_primary_account>;

-- 5. Verify all pipelines healthy
-- Check Dagster asset materializations, Snowpipe copy history
```

---

## Incident Response Checklist

- [ ] Outage detected (automated alert or manual)
- [ ] Incident commander assigned
- [ ] Continuity strategy selected (1/2/3)
- [ ] Failover executed
- [ ] Client redirect confirmed
- [ ] BI dashboards verified
- [ ] Kafka offset reconciliation confirmed
- [ ] Snowpipe COPY_HISTORY verified
- [ ] Dagster asset materializations resumed
- [ ] Stakeholders notified
- [ ] Post-incident review scheduled

---

## Escalation Matrix

| Severity | Response Time | Action |
|---|---|---|
| SEV1 (Full outage) | 5 minutes | Immediate failover, all-hands |
| SEV2 (Partial outage) | 15 minutes | Strategy 1 (reads only) |
| SEV3 (Degraded latency) | 1 hour | Monitor, prepare for failover |
| SEV4 (Non-critical) | Next business day | Investigate root cause |
