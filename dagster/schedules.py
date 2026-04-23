"""
Dagster Schedules — Cron-based triggers for jobs.
"""

from dagster import ScheduleDefinition


# Full ELT: Every 4 hours
elt_schedule = ScheduleDefinition(
    job_name="full_elt_pipeline",
    cron_schedule="0 */4 * * *",
    execution_timezone="UTC",
    description="Full ELT pipeline every 4 hours.",
)

# CDC streaming: Every 30 minutes
cdc_schedule = ScheduleDefinition(
    job_name="cdc_streaming_pipeline",
    cron_schedule="*/30 * * * *",
    execution_timezone="UTC",
    description="CDC/Streaming health check every 30 minutes.",
)

# Data quality: Twice daily
dq_schedule = ScheduleDefinition(
    job_name="data_quality_checks",
    cron_schedule="0 6,18 * * *",
    execution_timezone="UTC",
    description="Data quality checks at 06:00 and 18:00 UTC.",
)

# Maintenance: Daily at 03:00 UTC
maintenance_schedule = ScheduleDefinition(
    job_name="platform_maintenance",
    cron_schedule="0 3 * * *",
    execution_timezone="UTC",
    description="Daily maintenance at 03:00 UTC.",
)
