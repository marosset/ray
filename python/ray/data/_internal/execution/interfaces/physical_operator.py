import abc
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import ray
from .ref_bundle import RefBundle
from ray._raylet import ObjectRefGenerator
from ray.data._internal.execution.autoscaler.autoscaling_actor_pool import (
    AutoscalingActorPool,
)
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics
from ray.data._internal.logical.interfaces import LogicalOperator, Operator
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.stats import StatsDict, Timer
from ray.data.context import DataContext

if TYPE_CHECKING:

    from ray.data.block import BlockMetadataWithSchema

logger = logging.getLogger(__name__)


# TODO(hchen): Ray Core should have a common interface for these two types.
Waitable = Union[ray.ObjectRef, ObjectRefGenerator]


class OpTask(ABC):
    """Abstract class that represents a task that is created by an PhysicalOperator.

    The task can be either a regular task or an actor task.
    """

    def __init__(
        self,
        task_index: int,
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        self._task_index: int = task_index
        self._task_resource_bundle: Optional[ExecutionResources] = task_resource_bundle

    def task_index(self) -> int:
        """Return the index of the task."""
        return self._task_index

    def get_requested_resource_bundle(self) -> Optional[ExecutionResources]:
        return self._task_resource_bundle

    @abstractmethod
    def get_waitable(self) -> Waitable:
        """Return the ObjectRef or ObjectRefGenerator to wait on."""
        ...

    def _cancel(self, force: bool):
        object_ref = self.get_waitable()

        # Get generator's `ObjectRef`
        if isinstance(object_ref, ObjectRefGenerator):
            object_ref = object_ref._generator_ref

        is_actor_task = not object_ref.task_id().actor_id().is_nil()

        ray.cancel(
            object_ref,
            recursive=True,
            # NOTE: Actor tasks can't be force-cancelled
            force=force and not is_actor_task,
        )


class DataOpTask(OpTask):
    """Represents an OpTask that handles Block data."""

    def __init__(
        self,
        task_index: int,
        streaming_gen: ObjectRefGenerator,
        output_ready_callback: Callable[[RefBundle], None],
        task_done_callback: Callable[[Optional[Exception]], None],
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        """Create a DataOpTask
        Args:
            task_index: Index of the task. Used for callbacks.
            streaming_gen: The streaming generator of this task. It should yield blocks.
            output_ready_callback: The callback to call when a new RefBundle is output
                from the generator.
            task_done_callback: The callback to call when the task is done.
            task_resource_bundle: The execution resources of this task.
        """
        super().__init__(task_index, task_resource_bundle)
        # TODO(hchen): Right now, the streaming generator is required to yield a Block
        # and a BlockMetadata each time. We should unify task submission with an unified
        # interface. So each individual operator don't need to take care of the
        # BlockMetadata.
        self._streaming_gen = streaming_gen
        self._output_ready_callback = output_ready_callback
        self._task_done_callback = task_done_callback

    def get_waitable(self) -> ObjectRefGenerator:
        return self._streaming_gen

    def on_data_ready(self, max_bytes_to_read: Optional[int]) -> int:
        """Callback when data is ready to be read from the streaming generator.

        Args:
            max_bytes_to_read: Max bytes of blocks to read. If None, all available
                will be read.
        Returns: The number of blocks read.
        """
        bytes_read = 0
        while max_bytes_to_read is None or bytes_read < max_bytes_to_read:
            try:
                block_ref = self._streaming_gen._next_sync(0)
                if block_ref.is_nil():
                    # The generator currently doesn't have new output.
                    # And it's not stopped yet.
                    break
            except StopIteration:
                self._task_done_callback(None)
                break

            try:
                meta_with_schema: "BlockMetadataWithSchema" = ray.get(
                    next(self._streaming_gen)
                )
            except StopIteration:
                # The generator should always yield 2 values (block and metadata)
                # each time. If we get a StopIteration here, it means an error
                # happened in the task.
                # And in this case, the block_ref is the exception object.
                # TODO(hchen): Ray Core should have a better interface for
                # detecting and obtaining the exception.
                try:
                    ray.get(block_ref)
                    assert False, "Above ray.get should raise an exception."
                except Exception as ex:
                    self._task_done_callback(ex)
                    raise ex from None

            meta = meta_with_schema.metadata
            self._output_ready_callback(
                RefBundle(
                    [(block_ref, meta)],
                    owns_blocks=True,
                    schema=meta_with_schema.schema,
                ),
            )
            bytes_read += meta.size_bytes

        return bytes_read


class MetadataOpTask(OpTask):
    """Represents an OpTask that only handles metadata, instead of Block data."""

    def __init__(
        self,
        task_index: int,
        object_ref: ray.ObjectRef,
        task_done_callback: Callable[[], None],
        task_resource_bundle: Optional[ExecutionResources] = None,
    ):
        """
        Args:
            object_ref: The ObjectRef of the task.
            task_done_callback: The callback to call when the task is done.
        """
        super().__init__(task_index, task_resource_bundle)
        self._object_ref = object_ref
        self._task_done_callback = task_done_callback

    def get_waitable(self) -> ray.ObjectRef:
        return self._object_ref

    def on_task_finished(self):
        """Callback when the task is finished."""
        self._task_done_callback()


@dataclass
class _ActorPoolInfo:
    """Breakdown of the state of the actors used by the ``PhysicalOperator``"""

    running: int
    pending: int
    restarting: int

    def __str__(self):
        return (
            f"running={self.running}, restarting={self.restarting}, "
            f"pending={self.pending}"
        )


class PhysicalOperator(Operator):
    """Abstract class for physical operators.

    An operator transforms one or more input streams of RefBundles into a single
    output stream of RefBundles.

    Physical operators are stateful and non-serializable; they live on the driver side
    of the Dataset only.

    Here's a simple example of implementing a basic "Map" operator:

        class MapOperator(PhysicalOperator):
            def __init__(self):
                self.active_tasks = []

            def add_input(self, refs, _):
                self.active_tasks.append(map_task.remote(refs))

            def has_next(self):
                ready, _ = ray.wait(self.active_tasks, timeout=0)
                return len(ready) > 0

            def get_next(self):
                ready, remaining = ray.wait(self.active_tasks, num_returns=1)
                self.active_tasks = remaining
                return ready[0]

    Note that the above operator fully supports both bulk and streaming execution,
    since `add_input` and `get_next` can be called in any order. In bulk execution
    (now deprecated), all inputs would be added up-front, but in streaming
    execution (now the default execution mode) the calls could be interleaved.
    """

    _OPERATOR_ID_LABEL_KEY = "__data_operator_id"

    def __init__(
        self,
        name: str,
        input_dependencies: List["PhysicalOperator"],
        data_context: DataContext,
        target_max_block_size: Optional[int],
    ):
        super().__init__(name, input_dependencies)

        for x in input_dependencies:
            assert isinstance(x, PhysicalOperator), x
        self._inputs_complete = not input_dependencies
        self._output_block_size_option = None
        self.set_target_max_block_size(target_max_block_size)
        self._started = False
        self._shutdown = False
        self._in_task_submission_backpressure = False
        self._in_task_output_backpressure = False
        self._estimated_num_output_bundles = None
        self._estimated_output_num_rows = None
        self._execution_finished = False
        # The LogicalOperator(s) which were translated to create this PhysicalOperator.
        # Set via `PhysicalOperator.set_logical_operators()`.
        self._logical_operators: List[LogicalOperator] = []
        self._data_context = data_context
        self._id = str(uuid.uuid4())
        # Initialize metrics after data_context is set
        self._metrics = OpRuntimeMetrics(self)

    def __reduce__(self):
        raise ValueError("Operator is not serializable.")

    @property
    def id(self) -> str:
        """Return a unique identifier for this operator."""
        return self._id

    @property
    def data_context(self) -> DataContext:
        return self._data_context

    # Override the following 3 methods to correct type hints.

    @property
    def input_dependencies(self) -> List["PhysicalOperator"]:
        return super().input_dependencies  # type: ignore

    @property
    def output_dependencies(self) -> List["PhysicalOperator"]:
        return super().output_dependencies  # type: ignore

    def post_order_iter(self) -> Iterator["PhysicalOperator"]:
        return super().post_order_iter()  # type: ignore

    def set_logical_operators(
        self,
        *logical_ops: LogicalOperator,
    ):
        self._logical_operators = list(logical_ops)

    @property
    def target_max_block_size(self) -> Optional[int]:
        """
        Target max block size output by this operator. If this returns None,
        then the default from DataContext should be used.
        """
        if self._output_block_size_option is None:
            return None
        else:
            return self._output_block_size_option.target_max_block_size

    @property
    def actual_target_max_block_size(self) -> Optional[int]:
        """
        The actual target max block size output by this operator.
        Returns:
            `None` if the target max block size is not set, otherwise the target max block size.
            `None` means the block size is infinite.
        """
        target_max_block_size = self.target_max_block_size
        if target_max_block_size is None:
            target_max_block_size = self.data_context.target_max_block_size
        return target_max_block_size

    def set_target_max_block_size(self, target_max_block_size: Optional[int]):
        if target_max_block_size is not None:
            self._output_block_size_option = OutputBlockSizeOption(
                target_max_block_size=target_max_block_size
            )
        elif self._output_block_size_option is not None:
            self._output_block_size_option = None

    def mark_execution_finished(self):
        """Manually mark that this operator has finished execution."""
        self._execution_finished = True

    def execution_finished(self) -> bool:
        """Return True when this operator has finished execution.

        The outputs may or may not have been taken.
        """
        return self._execution_finished

    def completed(self) -> bool:
        """Returns whether this operator has been fully completed.

        An operator is completed iff:
            * The operator has finished execution (i.e., `execution_finished()` is True).
            * All outputs have been taken (i.e., `has_next()` is False) from it.
        """
        from ..operators.base_physical_operator import InternalQueueOperatorMixin

        internal_queue_size = (
            self.internal_queue_size()
            if isinstance(self, InternalQueueOperatorMixin)
            else 0
        )

        if not self._execution_finished:
            if (
                self._inputs_complete
                and internal_queue_size == 0
                and self.num_active_tasks() == 0
            ):
                # NOTE: Operator is considered completed iff
                #   - All input blocks have been ingested
                #   - Internal queue is empty
                #   - There are no active or pending tasks
                self._execution_finished = True

        return self._execution_finished and not self.has_next()

    def get_stats(self) -> StatsDict:
        """Return recorded execution stats for use with DatasetStats."""
        raise NotImplementedError

    @property
    def metrics(self) -> OpRuntimeMetrics:
        """Returns the runtime metrics of this operator."""
        self._metrics._extra_metrics = self._extra_metrics()
        return self._metrics

    def _extra_metrics(self) -> Dict[str, Any]:
        """Subclasses should override this method to report extra metrics
        that are specific to them."""
        return {}

    def _get_logical_args(self) -> Dict[str, Dict[str, Any]]:
        """Return the logical arguments that were translated to create this
        PhysicalOperator."""
        res = {}
        for i, logical_op in enumerate(self._logical_operators):
            logical_op_id = f"{logical_op}_{i}"
            res[logical_op_id] = logical_op._get_args()
        return res

    def progress_str(self) -> str:
        """Return any extra status to be displayed in the operator progress bar.

        For example, `<N> actors` to show current number of actors in an actor pool.
        """
        return ""

    def num_outputs_total(self) -> Optional[int]:
        """Returns the total number of output bundles of this operator,
        or ``None`` if unable to provide a reasonable estimate (for example,
        if no tasks have finished yet).

        The value returned may be an estimate based off the consumption so far.
        This is useful for reporting progress.

        Subclasses should either override this method, or update
        ``self._estimated_num_output_bundles`` appropriately.
        """
        return self._estimated_num_output_bundles

    def num_output_rows_total(self) -> Optional[int]:
        """Returns the total number of output rows of this operator,
        or ``None`` if unable to provide a reasonable estimate (for example,
        if no tasks have finished yet).

        The value returned may be an estimate based off the consumption so far.
        This is useful for reporting progress.

        Subclasses should either override this method, or update
        ``self._estimated_output_num_rows`` appropriately.
        """
        return self._estimated_output_num_rows

    def start(self, options: ExecutionOptions) -> None:
        """Called by the executor when execution starts for an operator.

        Args:
            options: The global options used for the overall execution.
        """
        self._started = True

    def should_add_input(self) -> bool:
        """Return whether it is desirable to add input to this operator right now.

        Operators can customize the implementation of this method to apply additional
        backpressure (e.g., waiting for internal actors to be created).
        """
        return True

    def add_input(self, refs: RefBundle, input_index: int) -> None:
        """Called when an upstream result is available.

        Inputs may be added in any order, and calls to `add_input` may be interleaved
        with calls to `get_next` / `has_next` to implement streaming execution.

        Subclasses should override `_add_input_inner` instead of this method.

        Args:
            refs: The ref bundle that should be added as input.
            input_index: The index identifying the input dependency producing the
                input. For most operators, this is always `0` since there is only
                one upstream input operator.
        """
        assert 0 <= input_index < len(self._input_dependencies), (
            f"Input index out of bounds (total inputs {len(self._input_dependencies)}, "
            f"index is {input_index})"
        )

        self._metrics.on_input_received(refs)
        self._add_input_inner(refs, input_index)

    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        """Subclasses should override this method to implement `add_input`."""
        raise NotImplementedError

    def input_done(self, input_index: int) -> None:
        """Called when the upstream operator at index `input_index` has completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for the given input index.
        """
        pass

    def all_inputs_done(self) -> None:
        """Called when all upstream operators have completed().

        After this is called, the executor guarantees that no more inputs will be added
        via `add_input` for any input index.
        """
        self._inputs_complete = True

    def has_next(self) -> bool:
        """Returns when a downstream output is available.

        When this returns true, it is safe to call `get_next()`.
        """
        raise NotImplementedError

    def get_next(self) -> RefBundle:
        """Get the next downstream output.

        It is only allowed to call this if `has_next()` has returned True.

        Subclasses should override `_get_next_inner` instead of this method.
        """
        output = self._get_next_inner()
        self._metrics.on_output_taken(output)
        return output

    def _get_next_inner(self) -> RefBundle:
        """Subclasses should override this method to implement `get_next`."""
        raise NotImplementedError

    def get_active_tasks(self) -> List[OpTask]:
        """Get a list of the active tasks of this operator.

        Subclasses should return *all* running normal/actor tasks. The
        StreamingExecutor will wait on these tasks and trigger callbacks.
        """
        return []

    def num_active_tasks(self) -> int:
        """Return the number of active tasks.

        This method is used for 2 purposes:
        * Determine if this operator is completed.
        * Displaying active task info in the progress bar.
        Thus, the return value can be less than `len(get_active_tasks())`,
        if some tasks are not needed for the above purposes. E.g., for the
        actor pool map operator, readiness checking tasks can be excluded
        from `num_active_tasks`, but they should be included in
        `get_active_tasks`.

        Subclasses can override this as a performance optimization.
        """
        return len(self.get_active_tasks())

    def throttling_disabled(self) -> bool:
        """Whether to disable resource throttling for this operator.

        This should return True for operators that only manipulate bundle metadata
        (e.g., the OutputSplitter operator). This hints to the execution engine that
        these operators should not be throttled based on resource usage.
        """
        return False

    def shutdown(self, timer: Timer, force: bool = False) -> None:
        """Abort execution and release all resources used by this operator.

        This release any Ray resources acquired by this operator such as active
        tasks, actors, and objects.
        """
        if self._shutdown:
            return
        elif not self._started:
            raise ValueError("Operator must be started before being shutdown.")

        # Mark operator as shut down
        self._shutdown = True
        # Time shutdown sequence duration
        with timer.timer():
            self._do_shutdown(force)

    def _do_shutdown(self, force: bool):
        # Default implementation simply cancels any outstanding active task
        self._cancel_active_tasks(force=force)

    def current_processor_usage(self) -> ExecutionResources:
        """Returns the current estimated CPU and GPU usage of this operator, excluding
        object store memory.

        This method is called by the executor to decide how to allocate processors
        between different operators.
        """
        return ExecutionResources(0, 0, 0)

    def running_processor_usage(self) -> ExecutionResources:
        """Returns the estimated running CPU and GPU usage of this operator, excluding
        object store memory.

        This method is called by the resource manager and the streaming
        executor to display the number of currently running CPUs and GPUs in the
        progress bar.

        Note, this method returns `current_processor_usage() -
        pending_processor_usage()` by default. Subclasses should only override
        `pending_processor_usage()` if needed.
        """
        usage = self.current_processor_usage()
        usage = usage.subtract(self.pending_processor_usage())
        return usage

    def pending_processor_usage(self) -> ExecutionResources:
        """Returns the estimated pending CPU and GPU usage of this operator, excluding
        object store memory.

        This method is called by the resource manager and the streaming
        executor to display the number of currently pending actors in the
        progress bar.
        """
        return ExecutionResources(0, 0, 0)

    def min_max_resource_requirements(
        self,
    ) -> Tuple[ExecutionResources, ExecutionResources]:
        """Returns the min and max resources to start the operator and make progress.

        For example, an operator that creates an actor pool requiring 8 GPUs could
        return ExecutionResources(gpu=8) as its minimum usage.

        This method is used by the resource manager to reserve minimum resources and to
        ensure that it doesn't over-provision resources.
        """
        return ExecutionResources.zero(), ExecutionResources.inf()

    def incremental_resource_usage(self) -> ExecutionResources:
        """Returns the incremental resources required for processing another input.

        For example, an operator that launches a task per input could return
        ExecutionResources(cpu=1) as its incremental usage.
        """
        return ExecutionResources()

    def notify_in_task_submission_backpressure(self, in_backpressure: bool) -> None:
        """Called periodically from the executor to update internal in backpressure
        status for stats collection purposes.

        Args:
            in_backpressure: Value this operator's in_backpressure should be set to.
        """
        # only update on change to in_backpressure
        if self._in_task_submission_backpressure != in_backpressure:
            self._metrics.on_toggle_task_submission_backpressure(in_backpressure)
            self._in_task_submission_backpressure = in_backpressure

    def notify_in_task_output_backpressure(self, in_backpressure: bool) -> None:
        """Called periodically from the executor to update internal output backpressure
        status for stats collection purposes.

        Args:
            in_backpressure: Value this operator's output backpressure should be set to.
        """
        # only update on change to in_backpressure
        if self._in_task_output_backpressure != in_backpressure:
            self._metrics.on_toggle_task_output_backpressure(in_backpressure)
            self._in_task_output_backpressure = in_backpressure

    def get_autoscaling_actor_pools(self) -> List[AutoscalingActorPool]:
        """Return a list of `AutoscalingActorPool`s managed by this operator."""
        return []

    def implements_accurate_memory_accounting(self) -> bool:
        """Return whether this operator implements accurate memory accounting.

        An operator that implements accurate memory accounting should properly
        report its memory usage via the following APIs:
          - `self._metrics.on_input_queued`.
          - `self._metrics.on_input_dequeued`.
          - `self._metrics.on_output_queued`.
          - `self._metrics.on_output_dequeued`.
        """
        # TODO(hchen): Currently we only enable `ReservationOpResourceAllocator` when
        # all operators in the dataset have implemented accurate memory accounting.
        # Eventually all operators should implement accurate memory accounting.
        return False

    def supports_fusion(self) -> bool:
        """Returns ```True``` if this operator can be fused with other operators."""
        return False

    def update_resource_usage(self) -> None:
        """Updates resource usage of this operator at runtime.

        This method will be called at runtime in each StreamingExecutor iteration.
        Subclasses can override it to account for dynamic resource usage updates due to
        restarting actors, retrying tasks, lost objects, etc.
        """
        pass

    def get_actor_info(self) -> _ActorPoolInfo:
        """Returns the current status of actors being used by the operator"""
        return _ActorPoolInfo(running=0, pending=0, restarting=0)

    def _cancel_active_tasks(self, force: bool):
        tasks: List[OpTask] = self.get_active_tasks()

        # Interrupt all (still) running tasks immediately
        for task in tasks:
            task._cancel(force=force)

        # In case of forced cancellation block until task actually return
        # to guarantee all tasks are done upon return from this method
        if force:
            # Wait for all tasks to get cancelled before returning
            for task in tasks:
                try:
                    ray.get(task.get_waitable())
                except ray.exceptions.RayError:
                    # Cancellation either succeeded, or the task might have already
                    # failed with a different error, or cancellation failed.
                    # In all cases, we swallow the exception.
                    pass

    def upstream_op_num_outputs(self):
        upstream_op_num_outputs = sum(
            op.num_outputs_total() or 0 for op in self.input_dependencies
        )
        return upstream_op_num_outputs


class ReportsExtraResourceUsage(abc.ABC):
    @abc.abstractmethod
    def extra_resource_usage(self: PhysicalOperator) -> ExecutionResources:
        """Returns resources used by this operator beyond standard accounting."""
        ...


def estimate_total_num_of_blocks(
    num_tasks_submitted: int,
    upstream_op_num_outputs: int,
    metrics: OpRuntimeMetrics,
    total_num_tasks: Optional[int] = None,
) -> Tuple[int, int, int]:
    """This method is trying to estimate total number of blocks/rows based on
    - How many outputs produced by the input deps
    - How many blocks/rows produced by tasks of this operator
    """

    if (
        upstream_op_num_outputs > 0
        and metrics.num_inputs_received > 0
        and metrics.num_tasks_finished > 0
    ):
        estimated_num_tasks = total_num_tasks
        if estimated_num_tasks is None:
            estimated_num_tasks = (
                upstream_op_num_outputs
                / metrics.num_inputs_received
                * num_tasks_submitted
            )

        estimated_num_output_bundles = round(
            estimated_num_tasks
            * metrics.num_outputs_of_finished_tasks
            / metrics.num_tasks_finished
        )
        estimated_output_num_rows = round(
            estimated_num_tasks
            * metrics.rows_task_outputs_generated
            / metrics.num_tasks_finished
        )
        return (
            estimated_num_tasks,
            estimated_num_output_bundles,
            estimated_output_num_rows,
        )

    return (0, 0, 0)


def _create_sub_pb(
    name: str, total_output_rows: Optional[int], position: int
) -> Tuple[ProgressBar, int]:
    progress_bar = ProgressBar(
        name,
        total_output_rows or 1,
        unit="row",
        position=position,
    )
    # NOTE: call `set_description` to trigger the initial print of progress
    # bar on console.
    progress_bar.set_description(f"  *- {name}")
    position += 1
    return progress_bar, position
