from collections import deque
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta
import re

from nightwatch.models.incident import LogSnippet


TIMESTAMP_PATTERN = re.compile(
    r"^\s*(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d{3,6})?)"
)

ERROR_MARKERS = (
    "oom",
    "outofmemoryerror",
    "container killed",
    "executorlostfailure",
    "exception",
    "error",
    "traceback",
)

RESOURCE_MARKERS = (
    "memory pressure",
    "memory usage",
    "heap space",
    "gc overhead",
    "spill",
    "peak",
    "rss",
    "backpressure",
)

STACKTRACE_PREFIXES = (
    "at ",
    "\tat ",
    "caused by:",
    "...",
    "org.apache",
    "java.",
)

RESOURCE_PERCENT_PATTERN = re.compile(r"\b(?:9\d|100)%\b")

WindowedLine = tuple[int, datetime, str, bool]


def parse_timestamp_from_line(line: str) -> datetime | None:
    match = TIMESTAMP_PATTERN.match(line)
    if not match:
        return None

    raw_timestamp = match.group(1).replace(",", ".")
    try:
        return datetime.fromisoformat(raw_timestamp)
    except ValueError:
        return None


def sliding_window(
    raw_lines: Iterable[str],
    failure_timestamp: datetime,
    seconds_before: int = 120,
    seconds_after: int = 120,
) -> Iterator[WindowedLine]:
    if seconds_before < 0 or seconds_after < 0:
        raise ValueError("window size cannot be negative")

    window_start = failure_timestamp - timedelta(seconds=seconds_before)
    window_end = failure_timestamp + timedelta(seconds=seconds_after)
    last_timestamp: datetime | None = None

    for index, raw_line in enumerate(raw_lines):
        line = raw_line.rstrip("\n")
        parsed_timestamp = parse_timestamp_from_line(line)

        if parsed_timestamp is not None:
            last_timestamp = parsed_timestamp
            effective_timestamp = parsed_timestamp
            has_explicit_timestamp = True
        elif last_timestamp is not None:
            effective_timestamp = last_timestamp
            has_explicit_timestamp = False
        else:
            continue

        if effective_timestamp < window_start or effective_timestamp > window_end:
            continue

        yield index, effective_timestamp, line, has_explicit_timestamp


def is_relevant_log_line(line: str) -> bool:
    lowered = line.lower()
    if any(marker in lowered for marker in ERROR_MARKERS):
        return True
    if any(marker in lowered for marker in RESOURCE_MARKERS):
        return True
    return RESOURCE_PERCENT_PATTERN.search(lowered) is not None


def is_stacktrace_continuation(line: str, has_explicit_timestamp: bool) -> bool:
    if has_explicit_timestamp:
        return False

    stripped = line.strip().lower()
    return any(stripped.startswith(prefix) for prefix in STACKTRACE_PREFIXES)


def score_log_line(line: str) -> float:
    lowered = line.lower()
    score = 0.0
    score += sum(1.0 for marker in ERROR_MARKERS if marker in lowered)
    score += sum(0.5 for marker in RESOURCE_MARKERS if marker in lowered)
    if RESOURCE_PERCENT_PATTERN.search(lowered):
        score += 0.75
    return score


def extract_relevant_log_slices(
    raw_lines: Iterable[str],
    failure_timestamp: datetime,
    source: str = "yarn",
    host: str = "",
    seconds_before: int = 120,
    seconds_after: int = 120,
    context_lines: int = 2,
    max_snippets: int = 20,
) -> list[LogSnippet]:
    if context_lines < 0:
        raise ValueError("context_lines cannot be negative")
    if max_snippets < 1:
        raise ValueError("max_snippets must be at least 1")

    previous_context: deque[WindowedLine] = deque(maxlen=context_lines)
    snippets: list[LogSnippet] = []

    active_lines: list[str] = []
    active_start: int | None = None
    active_end: int | None = None
    active_score = 0.0
    trailing_context_remaining = 0
    has_relevant_line = False

    def flush_active() -> None:
        nonlocal active_lines, active_start, active_end, active_score, trailing_context_remaining, has_relevant_line
        if not active_lines or active_start is None or active_end is None or not has_relevant_line:
            active_lines = []
            active_start = None
            active_end = None
            active_score = 0.0
            trailing_context_remaining = 0
            has_relevant_line = False
            return

        snippets.append(
            LogSnippet(
                source=source,
                host=host,
                offset_start=active_start,
                offset_end=active_end,
                content="\n".join(active_lines),
                score=round(active_score, 2),
            )
        )
        active_lines = []
        active_start = None
        active_end = None
        active_score = 0.0
        trailing_context_remaining = 0
        has_relevant_line = False

    def append_active(index: int, line: str) -> None:
        nonlocal active_start, active_end
        if active_end == index:
            return
        if active_start is None:
            active_start = index
        active_end = index
        active_lines.append(line)

    for index, effective_timestamp, line, has_explicit_timestamp in sliding_window(
        raw_lines=raw_lines,
        failure_timestamp=failure_timestamp,
        seconds_before=seconds_before,
        seconds_after=seconds_after,
    ):
        relevant = is_relevant_log_line(line)
        continuation = is_stacktrace_continuation(line, has_explicit_timestamp)

        if relevant:
            if not active_lines:
                for context_index, _, context_line, _ in previous_context:
                    append_active(context_index, context_line)
            append_active(index, line)
            active_score += score_log_line(line)
            has_relevant_line = True
            trailing_context_remaining = context_lines
        elif active_lines and (continuation or trailing_context_remaining > 0):
            append_active(index, line)
            if not continuation and trailing_context_remaining > 0:
                trailing_context_remaining -= 1
        elif active_lines:
            flush_active()

        previous_context.append((index, effective_timestamp, line, has_explicit_timestamp))

        if len(snippets) >= max_snippets:
            break

    flush_active()
    return snippets[:max_snippets]