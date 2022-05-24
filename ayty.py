from dataclasses import dataclass, field
import heapq
import time
from typing import Iterator

# -----------------------------------------------------------------------------
# Instructions


class Instruction():
    def __init__(self):
        """An Instruction yielded from a task indicates when should that task
        be continued again."""
        pass

    def ready(self) -> bool:
        """When this method returs true, task binds with this Instruction is
        ready to be run again, else that task should be skip in this loop step.
        """
        return True


class HardIdle(Instruction):
    def __init__(self):
        pass

    def ready(self) -> bool:
        """Never be ready, awaken only by explict awake call."""
        return False


class Sleep(Instruction):
    def __init__(self, duration_milisec: int = 0):
        """Put a task into sleep with time out, sleep duration is specified in
        milisecond."""
        self.wake_up_time = duration_milisec / 1000 + time.perf_counter()

    def ready(self) -> bool:
        now = time.perf_counter()
        return now > self.wake_up_time


# -----------------------------------------------------------------------------
# Wrappers

"""General coroutine handle"""
Task = Iterator[Instruction]


@dataclass(order=True)
class Wrapper:
    inst: Instruction
    task: Task = field(compare=False)


@dataclass(order=True)
class DelayedWrapper:
    """Wraping tasks with time out. This type of tasks are store in dedicated
    task queue.

    Attributes:
        wake_up_time: time to wake up the task in second.
        task: binded task to be wake up when the time comes.
    """
    wake_up_time: float
    task: Task = field(compare=False)


# -----------------------------------------------------------------------------
# Internal Variables

# global state
_is_running = False

_tasks: list[Wrapper] = []
# heap, specialized for sleeping task
_tasks_delayed: list[DelayedWrapper] = []
# manually idled tasks, won't be continued until awaken manually
_hard_idled: set[Task] = set()


# -----------------------------------------------------------------------------
# Task Managing

def add_task(task: Task):
    wrapper = Wrapper(Instruction(), task)
    _tasks.append(wrapper)


def awake(task: Task):
    """Wake up a hard idled task, and run it in next event loop."""
    if task not in _hard_idled:
        return

    _hard_idled.remove(task)
    add_task(task)


# -----------------------------------------------------------------------------
# Task Dispatching

def _default_hander(task: Task, inst: Instruction):
    wrapper = Wrapper(inst, task)
    _tasks.append(wrapper)


def _sleep_handler(task: Task, inst: Instruction):
    wrapper = DelayedWrapper(inst.wake_up_time, task)
    heapq.heappush(_tasks_delayed, wrapper)


def _hardidle_handler(task: Task, inst: Instruction):
    _hard_idled.add(task)


def _none_handler(_task, _inst):
    pass


_DISPATH_MAP = {
    Sleep: _sleep_handler,
    HardIdle: _hardidle_handler,
    type(None): _none_handler,
}


def dispatch(task: Task, inst: Instruction):
    """Dispatch task to idle list corresponding to the instruction."""
    handler = _DISPATH_MAP.get(type(inst), None) or _default_hander
    handler(task, inst)


# -----------------------------------------------------------------------------
# State Meta Info

def is_running() -> bool:
    """check if there currently are running tasks."""
    return _is_running


def finished() -> bool:
    return not _tasks and not _tasks_delayed


# -----------------------------------------------------------------------------
# Steping

def step_a_task(task: Task):
    try:
        inst = next(task)
    except StopIteration:
        pass
    else:
        dispatch(task, inst)


def steping_delayed():
    now = time.perf_counter()
    while _tasks_delayed:
        if _tasks_delayed[0].wake_up_time > now:
            break

        wrapper = heapq.heappop(_tasks_delayed)
        step_a_task(wrapper.task)


def steping_normal():
    curr = 0
    ed = len(_tasks)
    while curr < ed:
        wrapper = _tasks[curr]
        if not wrapper.inst.ready():
            curr += 1
            continue
        # delete wrapper from task list
        last_wrapper = _tasks.pop()
        if last_wrapper is not wrapper:
            _tasks[curr] = last_wrapper
        ed -= 1
        # first delet, then run, in case wrapper coming back to _tasks.
        step_a_task(wrapper.task)


def step():
    """Conduct a single event loop pass."""
    steping_delayed()
    steping_normal()


def loop():
    """Main event loop."""
    global _is_running
    while not finished():
        _is_running = True
        step()
    _is_running = False
