import dagster as dg

from cfa.stf.routine.dagster.execution import (
    azure_batch_2cpu_execution_config,
    azure_batch_4cpu_execution_config,
    azure_batch_64cpu_execution_config,
)

# ============================================================================
# SCHEDULES AND AUTOMATION CONDITION SENSORS
# ============================================================================


# Custom Automation Condition. Relies on use_user_code_server=True on the sensor
class IsWeekday(dg.AutomationCondition):
    def __init__(self, weekday: int):
        """
        Check if evaluation time falls on a specific weekday.
        This is is a simple evaluation, rather than a stateful operation,
        such as with cron_tick_passed().

        Args:
            weekday: 0=Monday, 1=Tuesday, 2=Wednesday, 3=Thursday,
                    4=Friday, 5=Saturday, 6=Sunday
        """
        self.weekday = weekday
        super().__init__()

    def evaluate(self, context: dg.AutomationContext) -> dg.AutomationResult:
        # If the current weekday is equal to the desired weekday,
        # return the candidate_subset -> a dagster context's "true" case
        if context.evaluation_time.weekday() == self.weekday:
            true_subset = context.candidate_subset
        else:
            true_subset = context.get_empty_subset()

        return dg.AutomationResult(context=context, true_subset=true_subset)

    @property
    def name(self) -> str:
        """Define the label that will appear in the UI"""
        days = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        return f"is_{days[self.weekday].lower()}"


eager_on_wed = (
    # We specifically don't want these to run unless it's Wednesday
    # 0=monday,1=tuesday,2=wednesday,etc.
    # Note this is different from cron which is 1-indexed
    dg.AutomationCondition.eager() & IsWeekday(2)
).with_label("eager_on_wed")


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
