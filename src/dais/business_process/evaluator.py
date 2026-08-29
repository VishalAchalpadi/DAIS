"""Evaluating a group's status is just a query against
control.process_monitor for the current business date - no separate
tracking table needed. A breach (SLA time passed, any member not yet
complete) fires through the same alert mechanism as DQ/control-gate
failures (see quality/dq_alerts.py).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from dais.business_process.models import BusinessProcessGroup
from dais.monitoring.ddl import ensure_process_monitor_table
from dais.resilience.connectors.base import DatabaseConnector

MemberState = str  # "met" | "in_progress" | "pending"
GroupState = str  # "pending" | "in_progress" | "met" | "breached"


@dataclass
class MemberStatus:
    pipeline_name: str
    success_layer: str
    state: MemberState
    last_status: str | None  # raw process_monitor status, if any run exists today


@dataclass
class BusinessProcessStatus:
    process_name: str
    state: GroupState
    sla_deadline: datetime
    now: datetime
    members: list[MemberStatus] = field(default_factory=list)


def _latest_status_today(
    connector: DatabaseConnector,
    schema: str,
    table: str,
    pipeline_name: str,
    step: str,
    business_date: date,
) -> str | None:
    row = connector.fetch_one(
        f'SELECT status FROM "{schema}"."{table}" '
        "WHERE pipeline_name = %(pipeline_name)s AND step = %(step)s "
        "AND started_at::date = %(business_date)s "
        "ORDER BY started_at DESC LIMIT 1",
        {"pipeline_name": pipeline_name, "step": step, "business_date": business_date},
    )
    return row[0] if row else None


def evaluate_business_process(
    group: BusinessProcessGroup,
    connector: DatabaseConnector,
    *,
    monitoring_schema: str = "control",
    monitoring_table: str = "process_monitor",
    business_date: date | None = None,
) -> BusinessProcessStatus:
    # "no pipeline has run yet" is a legitimate pending state, not an
    # error - the table may genuinely not exist yet.
    ensure_process_monitor_table(connector, monitoring_schema, monitoring_table)

    tz = ZoneInfo(group.sla.timezone)
    now = datetime.now(tz)
    effective_date = business_date or now.date()

    hour, minute = (int(part) for part in group.sla.complete_by.split(":"))
    sla_deadline = datetime.combine(effective_date, time(hour, minute), tzinfo=tz)
    sla_passed = now > sla_deadline

    members: list[MemberStatus] = []
    for member in group.members:
        last_status = _latest_status_today(
            connector, monitoring_schema, monitoring_table, member.pipeline_name, member.success_layer, effective_date
        )
        if last_status == "complete":
            state = "met"
        elif last_status is not None:
            state = "in_progress"
        else:
            state = "pending"
        members.append(
            MemberStatus(
                pipeline_name=member.pipeline_name,
                success_layer=member.success_layer,
                state=state,
                last_status=last_status,
            )
        )

    all_met = all(m.state == "met" for m in members)
    any_started = any(m.state != "pending" for m in members)

    if all_met:
        group_state: GroupState = "met"
    elif sla_passed:
        group_state = "breached"
    elif any_started:
        group_state = "in_progress"
    else:
        group_state = "pending"

    return BusinessProcessStatus(
        process_name=group.process_name,
        state=group_state,
        sla_deadline=sla_deadline,
        now=now,
        members=members,
    )
