from datetime import datetime, timedelta

from nightwatch.retrieval.slicer import extract_relevant_log_slices, sliding_window


def build_large_log_stream(failure_time: datetime) -> tuple[list[str], str]:
    start_time = failure_time - timedelta(seconds=1500)
    lines: list[str] = []
    outside_window_error = f"{(failure_time + timedelta(seconds=400)):%Y-%m-%d %H:%M:%S} ERROR OutOfMemoryError outside target window"

    for index in range(3000):
        current_time = start_time + timedelta(seconds=index)
        lines.append(f"{current_time:%Y-%m-%d %H:%M:%S} INFO heartbeat line {index}")

        if current_time == failure_time:
            lines.append(
                f"{current_time:%Y-%m-%d %H:%M:%S} ERROR ExecutorLostFailure: Container killed by YARN for exceeding memory limits"
            )
            lines.append("java.lang.OutOfMemoryError: Java heap space")
            lines.append("\tat org.apache.spark.shuffle.sort.SortShuffleWriter.write(SortShuffleWriter.java:51)")

        if current_time == failure_time + timedelta(seconds=400):
            lines.append(outside_window_error)

    return lines, outside_window_error


def test_extract_relevant_log_slices_hits_target_window_in_large_log_stream() -> None:
    failure_time = datetime(2026, 5, 4, 3, 0, 0)
    lines, outside_window_error = build_large_log_stream(failure_time)

    snippets = extract_relevant_log_slices(lines, failure_time, source="yarn", host="worker-01")

    assert snippets
    matched_snippet = next(
        snippet for snippet in snippets if "OutOfMemoryError" in snippet.content
    )
    assert "Container killed by YARN" in matched_snippet.content
    assert "SortShuffleWriter.write" in matched_snippet.content
    assert outside_window_error not in matched_snippet.content
    assert matched_snippet.score > 0
    assert matched_snippet.offset_start < matched_snippet.offset_end


def test_extract_relevant_log_slices_handles_empty_and_unparseable_input() -> None:
    failure_time = datetime(2026, 5, 4, 3, 0, 0)

    assert extract_relevant_log_slices([], failure_time) == []
    assert extract_relevant_log_slices(["INFO no timestamp here", "still no timestamp"], failure_time) == []


def test_sliding_window_only_yields_lines_inside_failure_window() -> None:
    failure_time = datetime(2026, 5, 4, 3, 0, 0)
    lines = [
        "2026-05-04 02:57:30 INFO before window",
        "2026-05-04 02:58:30 INFO inside window",
        "stacktrace continuation without timestamp",
        "2026-05-04 03:01:30 ERROR inside window",
        "2026-05-04 03:03:30 INFO after window",
    ]

    window_lines = list(sliding_window(lines, failure_time, seconds_before=120, seconds_after=120))

    assert len(window_lines) == 3
    assert window_lines[0][2].endswith("inside window")
    assert window_lines[1][2] == "stacktrace continuation without timestamp"
    assert window_lines[2][2].endswith("inside window")