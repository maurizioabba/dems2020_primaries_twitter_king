"""
Sensor checking a set of status for a dag run  in a specified time interval.

The sensor is a refinement of the ExternalTaskSensor.

The poke function is slightly modified to check if there is any instance
of the task_id (or dag_id) selected completed over the last X seconds.

Note that our reference sensor is the ExternalTaskSensor from
airflow version 1.10.3.
"""
import os

from airflow.exceptions import AirflowException
from airflow.models import TaskInstance, DagBag, DagModel, DagRun
from airflow.operators.sensors import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.utils import timezone

class LastExternalTaskSensor(BaseSensorOperator):
    """
    Modification of the ExternalTaskSensor to check if the specified
    task_id or dag_id is in the required state over the last N seconds
    rather than in specified datetime.

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: str
    :param external_task_id: The task_id that contains the task you want to
        wait for. If ``None`` the sensor waits for the DAG
    :type external_task_id: str
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param execution_delta: max time difference to check with respect to
        current time. Either execution_delta or execution_date_fn can
        be passed to ExternalTaskSensor, but not both.
    :type execution_delta: datetime.timedelta
    :param execution_date_fn: function that receives the current execution date
        and returns the desired execution_delta to check. Either execution_delta
        or execution_date_fn can be passed to ExternalTaskSensor, but not both.
    :type execution_date_fn: callable
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    :type check_existence: bool
    """
    template_fields = ['external_dag_id', 'external_task_id']
    ui_color = '#19647e'

    @apply_defaults
    def __init__(self,
                 external_dag_id,
                 external_task_id,
                 allowed_states=None,
                 execution_delta=None,
                 execution_date_fn=None,
                 check_existence=False,
                 *args,
                 **kwargs):
        """
        ExternalTaskSensor.__init__ from 1.10.3.

        TODO: remove this before commit
        """
        super(LastExternalTaskSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        if external_task_id:
            if not set(self.allowed_states) <= set(State.task_states):
                raise ValueError(
                    'Valid values for `allowed_states` '
                    'when `external_task_id` is not `None`: {}'.format(State.task_states)
                )
        else:
            if not set(self.allowed_states) <= set(State.dag_states):
                raise ValueError(
                    'Valid values for `allowed_states` '
                    'when `external_task_id` is `None`: {}'.format(State.dag_states)
                )

        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_delta` or `execution_date_fn` may '
                'be provided to ExternalTaskSensor; not both.')

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.check_existence = check_existence
        # we only check the existence for the first time.
        self.has_checked_existence = False

    @provide_session
    def poke(self, context, session=None):
        """
        Check if specified task_id (dag_id) completed over the specified timedelta.

        Slight variation of LastExternalTaskSensor.poke function to
        check if at least one task_id (dag_id) completed over the time
        interval going from now to the specified one (either via
        execution_delta or execution_date_fn.
        """
        curr_datetime = timezone.utcnow()

        if self.execution_delta:
            min_datetime = curr_datetime - self.execution_delta
        elif self.execution_date_fn:
            min_datetime = curr_datetime - self.execution_date_fn(context['execution_date'])
        else:
            min_datetime = context['execution_date']

        self.log.info(
            'Poking for %s.%s in state %s on timedelta %s-%s ... ',
            self.external_dag_id, self.external_task_id, self.allowed_states,
            min_datetime, curr_datetime
        )

        DM = DagModel
        TI = TaskInstance
        DR = DagRun

        # NOTE: v 1.10.3 introduced "check_existence" and "has_checked_existence"

        # we only do the check for 1st time, no need for subsequent poke
        if self.check_existence and not self.has_checked_existence:
            dag_to_wait = session.query(DM).filter(
                DM.dag_id == self.external_dag_id
            ).first()

            if not dag_to_wait:
                raise AirflowException('The external DAG '
                                       '{} does not exist.'.format(self.external_dag_id))
            else:
                if not os.path.exists(dag_to_wait.fileloc):
                    raise AirflowException('The external DAG '
                                           '{} was deleted.'.format(self.external_dag_id))

            if self.external_task_id:
                refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
                if not refreshed_dag_info.has_task(self.external_task_id):
                    raise AirflowException('The external task'
                                           '{} in DAG {} does not exist.'.format(
                                               self.external_task_id,
                                               self.external_dag_id))
            self.has_checked_existence = True

        if self.external_task_id:
            count = session.query(TI).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                DR.execution_date.between(min_datetime, curr_datetime),
            ).count()
        else:
            count = session.query(DR).filter(
                DR.dag_id == self.external_dag_id,
                DR.state.in_(self.allowed_states),
                DR.execution_date.between(min_datetime, curr_datetime),
            ).count()

        self.log.info("found %s tasks for the requested query", count)
        session.commit()
        return bool(count)

class LastExternalTaskPlugin(AirflowPlugin):
    name = "last_external_task"
    operators = [LastExternalTaskSensor]

