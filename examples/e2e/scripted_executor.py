"""Deterministic AgentExecutor used by the end-to-end example.

The executor is intentionally tiny and predictable so the e2e test (and any
human running the scenarios in ``client.py``) can make exact assertions
against the resulting Task / artifact / status sequence.

Inbound command (extracted from the first text Part of ``context.message``):

* ``"fail"``  -> SUBMITTED -> WORKING -> FAILED (no artifact)
* ``"wait"``  -> SUBMITTED -> WORKING -> blocks on an internal ``asyncio.Event``
                 until ``cancel()`` is invoked; cancel then drives the
                 CANCELED terminal state.
* anything else (default echo flow) ->
                 SUBMITTED -> WORKING -> emit an artifact named ``"result"``
                 whose text is ``<UPPER_INPUT> (processed)`` -> COMPLETED.

The class is deliberately stateful (a single in-process registry of pending
wait events keyed by task_id) so cancel() can resolve the matching execute()
invocation.
"""

from __future__ import annotations

import asyncio
import logging

from a2a.helpers.proto_helpers import get_message_text
from a2a.server.agent_execution.agent_executor import AgentExecutor
from a2a.server.agent_execution.context import RequestContext
from a2a.server.events.event_queue_v2 import EventQueue
from a2a.server.tasks.task_updater import TaskUpdater
from a2a.types.a2a_pb2 import Part, Task, TaskState, TaskStatus


logger = logging.getLogger(__name__)


class ScriptedAgentExecutor(AgentExecutor):
    """Deterministic AgentExecutor for the redis-backed e2e example."""

    def __init__(self) -> None:
        # Maps task_id -> asyncio.Event used to unblock the "wait" command
        # from cancel(). A simple dict is safe because the server processes
        # at most one execute() per task at a time.
        self._wait_events: dict[str, asyncio.Event] = {}

    # ------------------------------------------------------------------
    # AgentExecutor ABC
    # ------------------------------------------------------------------

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Drive the task lifecycle based on the inbound text command."""
        message = context.message
        task_id = context.task_id or ""
        context_id = context.context_id or ""
        if not message or not task_id or not context_id:
            return

        command = get_message_text(message).strip()
        # "slow" mode lets clients race-free register push configs before the
        # terminal COMPLETED status fires: it inserts long enough sleeps to
        # cover any local config-registration round trip.
        slow_step_s = 0.5 if command.startswith("slow:") else 0.01
        if command.startswith("slow:"):
            command = command[len("slow:") :]

        updater = TaskUpdater(
            event_queue=event_queue,
            task_id=task_id,
            context_id=context_id,
        )

        # Enqueue the initial Task object before emitting status updates --
        # the EventConsumer requires the Task to be created first so that
        # TaskStatusUpdateEvent can attach to an existing task in storage.
        if context.current_task is None:
            await event_queue.enqueue_event(
                Task(
                    id=task_id,
                    context_id=context_id,
                    status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
                )
            )

        # SUBMITTED -> brief pause -> WORKING. The sleep gives the test the
        # opportunity to observe the intermediate state via streaming.
        await updater.submit()
        await asyncio.sleep(slow_step_s)
        await updater.start_work()

        if command == "fail":
            logger.info("ScriptedAgentExecutor: failing task %s", task_id)
            await updater.failed()
            return

        if command == "wait":
            event = asyncio.Event()
            self._wait_events[task_id] = event
            logger.info(
                "ScriptedAgentExecutor: task %s waiting for cancel",
                task_id,
            )
            try:
                await event.wait()
            finally:
                self._wait_events.pop(task_id, None)
            # cancel() is responsible for emitting the CANCELED terminal
            # state, so we simply return here.
            return

        # Default echo flow: emit the deterministic artifact, then complete.
        result_text = f"{command.upper()} (processed)"
        await updater.add_artifact(
            parts=[Part(text=result_text)],
            name="result",
            last_chunk=True,
        )
        # Brief pause so external observers (push-config registrars in the
        # multi-owner dispatch scenario) have a window to wire up before the
        # terminal COMPLETED status fires.
        await asyncio.sleep(slow_step_s)
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        """Unblock any waiting execute() and emit a CANCELED status."""
        task_id = context.task_id or ""
        context_id = context.context_id or ""

        # Resolve the wait barrier first so the still-running execute()
        # finishes promptly. cancel() takes responsibility for emitting the
        # terminal state on its own TaskUpdater instance.
        wait_event = self._wait_events.get(task_id)
        if wait_event is not None:
            wait_event.set()

        updater = TaskUpdater(
            event_queue=event_queue,
            task_id=task_id,
            context_id=context_id,
        )
        await updater.cancel()
