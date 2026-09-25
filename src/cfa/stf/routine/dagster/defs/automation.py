"Automation for our dagster assets."

from calendar import Day
from zoneinfo import ZoneInfo

import dagster as dg

from cfa.stf.routine.dagster.defs.execution import (
    azure_batch_2cpu_execution_config,
    azure_batch_4cpu_execution_config,
    azure_batch_64cpu_execution_config,
)

# ============================================================================
# SCHEDULES AND AUTOMATION CONDITION SENSORS
# ============================================================================

NEW_YORK = ZoneInfo("America/New_York")


class IsWeekday(dg.AutomationCondition):
    """
    Check if evaluation time falls on a specific weekday.
    This is a simple evaluation, rather than a stateful operation,
    such as with cron_tick_passed().

    Args:
        weekday: Day.MONDAY, Day.TUESDAY, Day.WEDNESDAY, etc.
    """

    def __init__(self, weekday: Day):
        self.weekday = weekday

    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        evaluation_time = context.evaluation_time.astimezone(NEW_YORK)

        return dg.AutomationResult(
            context=context,
            true_subset=(
                context.candidate_subset
                if evaluation_time.weekday() == self.weekday
                else context.get_empty_subset()
            ),
        )

    @property
    def name(self) -> str:
        "Define the label that will appear in UI"
        return f"is_{self.weekday.name.lower()}"


eager_on_wednesday = (
    dg.AutomationCondition.eager() & IsWeekday(Day.WEDNESDAY)
).with_label("eager_on_wednesday")


fable_sensor = dg.AutomationConditionSensorDefinition(
    name="Fable",
    target=dg.AssetSelection.groups("Fable"),
    run_tags=azure_batch_2cpu_execution_config.to_run_tags(),
    use_user_code_server=True,  # allows for custom automation conditions
)

pyrenew_sensor = dg.AutomationConditionSensorDefinition(
    name="Pyrenew",
    target=dg.AssetSelection.groups("Pyrenew"),
    run_tags=azure_batch_4cpu_execution_config.to_run_tags(),
    use_user_code_server=True,  # allows for custom automation conditions
)

fusion_sensor = dg.AutomationConditionSensorDefinition(
    name="Fusion",
    target=dg.AssetSelection.groups("Fusion"),
    run_tags=azure_batch_2cpu_execution_config.to_run_tags(),
    use_user_code_server=True,  # allows for custom automation conditions
)

postprocess_sensor = dg.AutomationConditionSensorDefinition(
    name="Postprocess",
    target=dg.AssetSelection.groups("Postprocess"),
    run_tags=azure_batch_2cpu_execution_config.to_run_tags(),
    use_user_code_server=True,  # allows for custom automation conditions
)

epiautogp_sensor = dg.AutomationConditionSensorDefinition(
    name="EpiAutoGP",
    target=dg.AssetSelection.groups("EpiAutoGP"),
    run_tags=azure_batch_64cpu_execution_config.to_run_tags(),
    use_user_code_server=True,  # allows for custom automation conditions
)
