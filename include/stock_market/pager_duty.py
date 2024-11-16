from airflow.providers.pagerduty.hooks.pagerduty_events import PagerdutyEventsHook


class PagerDuty():
    """
    Testing PagerDuty events hook
    """
    @staticmethod
    def pager_duty_callback(context: dict):
        """
        Callback.
        """
        dag = context.get("dag")
        task = context.get("task_instance")
        exception = context.get("exception")

        pg_conn = PagerdutyEventsHook(
            pagerduty_events_conn_id="pager_duty_conn")

        # Test connection to PagerDuty endpoint
        pg_conn.test_connection()

        return pg_conn.send_event(
            summary=f"Task {task.task_id} failed from dag {dag.dag_id}",
            severity="error",
            source="Airflow",
            custom_details=f"Error message: {exception}"
        )
