from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

RUNNER_FILE = SCRIPT_DIR / "pytest_runner.py"
REPORTS_DIR = PROJECT_ROOT / "reports"

PYLINT_XML = REPORTS_DIR / "pylint-report.xml"
PYLINT_LOG = REPORTS_DIR / "pylint.log"

MYPY_XML = REPORTS_DIR / "mypy-report.xml"
MYPY_LOG = REPORTS_DIR / "mypy.log"

PYTEST_LOG = REPORTS_DIR / "pytest.log"


class Color:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"


def color_text(text: str, color: str) -> str:
    return f"{color}{text}{Color.RESET}"


def ensure_reports_dir() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def run_git_command(args: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def get_commit_info() -> dict[str, str]:
    info = {
        "commit": "NO_COMMIT_AVAILABLE",
        "branch": "UNKNOWN_BRANCH",
        "status": "UNKNOWN_STATUS",
    }

    code, stdout, _ = run_git_command(["rev-parse", "--is-inside-work-tree"])
    if code != 0 or stdout.lower() != "true":
        info["status"] = "NOT_A_GIT_REPOSITORY"
        return info

    code, stdout, _ = run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])
    if code == 0 and stdout:
        info["branch"] = stdout

    code, stdout, _ = run_git_command(["rev-parse", "HEAD"])
    if code == 0 and stdout:
        info["commit"] = stdout
        info["status"] = "COMMIT_FOUND"
    else:
        info["status"] = "NO_COMMIT_YET"

    return info


def cleanup_old_pytest_xml_reports() -> None:
    ensure_reports_dir()
    for xml_file in REPORTS_DIR.glob("*.xml"):
        if xml_file.name in {PYLINT_XML.name, MYPY_XML.name}:
            continue
        xml_file.unlink(missing_ok=True)


def resolve_manual_target(target: str | None) -> Path | None:
    if not target:
        return None

    raw = Path(target)
    if raw.is_absolute():
        return raw.resolve()
    return (PROJECT_ROOT / raw).resolve()


def get_manual_python_target(target: str | None, tool_name: str) -> list[Path] | None:
    resolved_target = resolve_manual_target(target)
    if resolved_target is None:
        return None

    if not resolved_target.exists() or not resolved_target.is_file():
        print(
            f"{color_text('WARNING:', Color.YELLOW)} "
            f"Manual target for {tool_name} not found: {resolved_target}"
        )
        return []

    if resolved_target.suffix != ".py":
        print(
            f"{color_text('WARNING:', Color.YELLOW)} "
            f"Manual target is not a Python file for {tool_name}: {resolved_target}"
        )
        return []

    return [resolved_target]


def run_pytest(target: str | None = None, enable_allure: bool = False) -> int:
    if not RUNNER_FILE.exists():
        print(f"ERROR: Runner file not found: {RUNNER_FILE}")
        return 1

    cleanup_old_pytest_xml_reports()

    cmd = [sys.executable, str(RUNNER_FILE)]
    if target:
        cmd.extend(["--target", target])
    if enable_allure:
        cmd.append("--enable-allure")

    result = subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    with PYTEST_LOG.open("w", encoding="utf-8") as handle:
        handle.write("STDOUT\n")
        handle.write("=" * 80 + "\n")
        handle.write(result.stdout or "")
        handle.write("\n\nSTDERR\n")
        handle.write("=" * 80 + "\n")
        handle.write(result.stderr or "")
        handle.write("\n")

    return result.returncode


def parse_pytest_xml(xml_file: Path) -> dict:
    tree = ET.parse(xml_file)
    root = tree.getroot()

    suites = [root] if root.tag == "testsuite" else root.findall("testsuite")

    total = 0
    failures = 0
    errors = 0
    skipped = 0
    testcases = []
    placeholder_reason = ""

    for suite in suites:
        total += int(suite.attrib.get("tests", 0))
        failures += int(suite.attrib.get("failures", 0))
        errors += int(suite.attrib.get("errors", 0))
        skipped += int(suite.attrib.get("skipped", 0))

        properties = suite.find("properties")
        if properties is not None:
            for prop in properties.findall("property"):
                if prop.attrib.get("name") == "reason":
                    placeholder_reason = prop.attrib.get("value", "")

        system_out = suite.findtext("system-out", default="").strip()
        if not placeholder_reason and system_out:
            placeholder_reason = system_out

        for testcase in suite.findall("testcase"):
            name = testcase.attrib.get("name", "unknown")
            status = "passed"
            details = ""

            failure_node = testcase.find("failure")
            error_node = testcase.find("error")
            skipped_node = testcase.find("skipped")

            if failure_node is not None:
                status = "failed"
                details = (
                    failure_node.attrib.get("message")
                    or failure_node.text
                    or ""
                ).strip()
            elif error_node is not None:
                status = "error"
                details = (
                    error_node.attrib.get("message")
                    or error_node.text
                    or ""
                ).strip()
            elif skipped_node is not None:
                status = "skipped"
                details = (
                    skipped_node.attrib.get("message")
                    or skipped_node.text
                    or ""
                ).strip()

            testcases.append(
                {
                    "name": name,
                    "status": status,
                    "details": details,
                }
            )

    return {
        "file": xml_file.name,
        "total": total,
        "passed": total - failures - errors - skipped,
        "failed": failures,
        "errors": errors,
        "skipped": skipped,
        "testcases": testcases,
        "placeholder_reason": placeholder_reason,
    }


def print_pytest_summary(
    results: list[dict],
    commit_info: dict[str, str],
) -> None:
    print(f"\n{color_text('========== PYTEST REPORT ==========', Color.BLUE)}\n")
    print(f"Branch  : {commit_info['branch']}")
    print(f"Commit  : {commit_info['commit']}")
    print(f"Git     : {commit_info['status']}")

    if not results:
        print(
            f"\n{color_text('WARNING:', Color.YELLOW)} "
            "No XML reports found for pytest."
        )
        print(f"Pytest log: {PYTEST_LOG.relative_to(PROJECT_ROOT)}")
        print(f"\n{color_text('===================================', Color.BLUE)}\n")
        return

    total = 0
    passed = 0
    failed = 0
    errors = 0
    skipped = 0
    placeholder_notes: list[str] = []
    all_testcases: list[dict[str, str]] = []

    for result in results:
        total += result["total"]
        passed += result["passed"]
        failed += result["failed"]
        errors += result["errors"]
        skipped += result["skipped"]

        if result.get("placeholder_reason") and result["total"] == 0:
            placeholder_notes.append(
                f"{result['file']}: {result['placeholder_reason']}"
            )

        for tc in result["testcases"]:
            all_testcases.append(
                {
                    "file": result["file"],
                    "name": tc["name"],
                    "status": tc["status"],
                    "details": tc["details"],
                }
            )

    print(f"\n{color_text('Total   :', Color.BLUE)} {total}")
    print(f"{color_text('Passed  :', Color.GREEN)} {passed}")
    print(f"{color_text('Failed  :', Color.RED)} {failed}")
    print(f"{color_text('Errors  :', Color.RED)} {errors}")
    print(f"{color_text('Warnings:', Color.YELLOW)} {skipped}")

    if all_testcases:
        print(f"\n{color_text('All Test Cases:', Color.BLUE)}")
        for tc in all_testcases:
            status = tc["status"]
            if status == "passed":
                label = color_text("[passed]", Color.GREEN)
            elif status in {"failed", "error"}:
                label = color_text(f"[{status}]", Color.RED)
            else:
                label = color_text("[warning]", Color.YELLOW)

            print(f"  - {tc['name']} {label}")
            if tc["details"] and tc["status"] != "passed":
                print(f"    {tc['details']}")

    if placeholder_notes:
        print(f"\n{color_text('Placeholder Reports:', Color.YELLOW)}")
        for note in placeholder_notes:
            print(f"  - {note}")

    print(f"\nFull pytest log: {PYTEST_LOG.relative_to(PROJECT_ROOT)}")
    print(f"\n{color_text('===================================', Color.BLUE)}\n")


def get_recently_changed_python_files() -> list[Path]:
    changed_files: set[str] = set()

    git_queries = [
        ["diff", "--name-only"],
        ["diff", "--cached", "--name-only"],
        ["ls-files", "--others", "--exclude-standard"],
    ]

    for query in git_queries:
        code, stdout, _ = run_git_command(query)
        if code != 0:
            continue

        for item in stdout.splitlines():
            item = item.strip()
            if item.endswith(".py"):
                changed_files.add(item)

    paths = []
    for item in sorted(changed_files):
        path = (PROJECT_ROOT / item).resolve()
        if path.exists() and path.is_file():
            paths.append(path)

    return paths


def extract_pylint_score(output: str) -> str:
    score_patterns = [
        r"rated at\s+(-?\d+(?:\.\d+)?)\/10",
        r"Your code has been rated at\s+(-?\d+(?:\.\d+)?)\/10",
    ]

    for pattern in score_patterns:
        match = re.search(pattern, output, flags=re.IGNORECASE)
        if match:
            return f"{match.group(1)}/10"

    return "N/A"


def run_pylint_command(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def run_pylint(target: str | None = None) -> int:
    ensure_reports_dir()

    pylint_exe = shutil.which("pylint")
    if pylint_exe is None:
        print(f"{color_text('WARNING:', Color.YELLOW)} pylint not found in PATH.")
        create_empty_pylint_xml("pylint not installed")
        PYLINT_LOG.write_text("pylint not installed\n", encoding="utf-8")
        return 0

    manual_targets = get_manual_python_target(target, "pylint")
    targets = (
        manual_targets
        if manual_targets is not None
        else get_recently_changed_python_files()
    )

    if not targets:
        print(
            f"{color_text('WARNING:', Color.YELLOW)} "
            "No recently changed Python files found by git for pylint."
        )
        create_empty_pylint_xml("no recently changed python files found")
        PYLINT_LOG.write_text(
            "no recently changed python files found\n",
            encoding="utf-8",
        )
        return 0

    print(
        color_text(
            "[INFO] Running pylint on recently changed Python files:",
            Color.BLUE,
        )
    )
    for target_path in targets:
        try:
            rel = target_path.relative_to(PROJECT_ROOT)
        except ValueError:
            rel = target_path
        print(f"  - {rel}")

    parseable_cmd = [
        pylint_exe,
        "--output-format=parseable",
        "--score=y",
        *[str(path) for path in targets],
    ]
    score_cmd = [
        pylint_exe,
        "--score=y",
        *[str(path) for path in targets],
    ]

    parseable_result = run_pylint_command(parseable_cmd)
    score_result = run_pylint_command(score_cmd)
    pylint_score = extract_pylint_score(
        "\n".join(
            [
                score_result.stdout or "",
                score_result.stderr or "",
                parseable_result.stdout or "",
                parseable_result.stderr or "",
            ]
        )
    )

    with PYLINT_LOG.open("w", encoding="utf-8") as handle:
        handle.write("PARSEABLE STDOUT\n")
        handle.write("=" * 80 + "\n")
        handle.write(parseable_result.stdout or "")
        handle.write("\n\nPARSEABLE STDERR\n")
        handle.write("=" * 80 + "\n")
        handle.write(parseable_result.stderr or "")
        handle.write("\n\nSCORE STDOUT\n")
        handle.write("=" * 80 + "\n")
        handle.write(score_result.stdout or "")
        handle.write("\n\nSCORE STDERR\n")
        handle.write("=" * 80 + "\n")
        handle.write(score_result.stderr or "")
        handle.write("\n")

    print(
        f"{color_text('[INFO]', Color.BLUE)} "
        f"Full pylint log saved to: {PYLINT_LOG.relative_to(PROJECT_ROOT)}"
    )

    write_pylint_xml(
        parseable_stdout=parseable_result.stdout,
        parseable_stderr=parseable_result.stderr,
        targets=targets,
        pylint_score=pylint_score,
    )
    return parseable_result.returncode


def write_pylint_xml(
    parseable_stdout: str,
    parseable_stderr: str,
    targets: list[Path],
    pylint_score: str,
) -> None:
    commit_info = get_commit_info()

    testsuite = ET.Element("testsuite")
    testsuite.set("name", "pylint")
    testsuite.set("tests", str(len(targets)))

    failures = 0
    issues_by_file: dict[str, list[str]] = {}

    for line in parseable_stdout.splitlines():
        line = line.strip()
        if not line:
            continue

        file_key = line.split(":", 1)[0].strip()
        issues_by_file.setdefault(file_key, []).append(line)

    properties = ET.SubElement(testsuite, "properties")

    score_prop = ET.SubElement(properties, "property")
    score_prop.set("name", "pylint_score")
    score_prop.set("value", pylint_score)

    for target in targets:
        try:
            rel = str(target.relative_to(PROJECT_ROOT))
        except ValueError:
            rel = str(target)

        testcase = ET.SubElement(testsuite, "testcase")
        testcase.set("classname", "pylint")
        testcase.set("name", rel)

        issues = (
            issues_by_file.get(str(target), [])
            or issues_by_file.get(rel, [])
        )
        if issues:
            failures += 1
            failure = ET.SubElement(testcase, "failure")
            failure.set("message", f"{len(issues)} pylint issue(s)")
            failure.text = "\n".join(issues)

    testsuite.set("failures", str(failures))
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    system_out = ET.SubElement(testsuite, "system-out")
    system_out.text = (
        f"branch={commit_info['branch']}\n"
        f"commit={commit_info['commit']}\n"
        f"git_status={commit_info['status']}\n"
        f"pylint_score={pylint_score}\n"
        f"{parseable_stderr.strip()}".strip()
    )

    tree = ET.ElementTree(testsuite)
    tree.write(PYLINT_XML, encoding="utf-8", xml_declaration=True)


def create_empty_pylint_xml(reason: str) -> None:
    commit_info = get_commit_info()

    testsuite = ET.Element("testsuite")
    testsuite.set("name", "pylint")
    testsuite.set("tests", "0")
    testsuite.set("failures", "0")
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    properties = ET.SubElement(testsuite, "properties")
    score_prop = ET.SubElement(properties, "property")
    score_prop.set("name", "pylint_score")
    score_prop.set("value", "N/A")

    system_out = ET.SubElement(testsuite, "system-out")
    system_out.text = (
        f"branch={commit_info['branch']}\n"
        f"commit={commit_info['commit']}\n"
        f"git_status={commit_info['status']}\n"
        f"pylint_score=N/A\n"
        f"reason={reason}"
    )

    tree = ET.ElementTree(testsuite)
    tree.write(PYLINT_XML, encoding="utf-8", xml_declaration=True)


def get_pylint_score_from_xml(root: ET.Element) -> str:
    properties = root.find("properties")
    if properties is not None:
        for prop in properties.findall("property"):
            if prop.attrib.get("name") == "pylint_score":
                return prop.attrib.get("value", "N/A")

    system_out = root.findtext("system-out", default="")
    match = re.search(r"pylint_score=(.+)", system_out)
    if match:
        return match.group(1).strip()

    return "N/A"


def print_pylint_summary(commit_info: dict[str, str]) -> None:
    print(f"\n{color_text('========== PYLINT REPORT ==========', Color.BLUE)}\n")
    print(f"Branch  : {commit_info['branch']}")
    print(f"Commit  : {commit_info['commit']}")
    print(f"Git     : {commit_info['status']}")

    if not PYLINT_XML.exists():
        print(f"\n{color_text('WARNING:', Color.YELLOW)} No pylint XML report found.")
        print(f"\n{color_text('===================================', Color.BLUE)}\n")
        return

    try:
        tree = ET.parse(PYLINT_XML)
        root = tree.getroot()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(
            f"\n{color_text('WARNING:', Color.YELLOW)} "
            f"Failed to parse pylint XML report: {exc}"
        )
        print(f"\n{color_text('===================================', Color.BLUE)}\n")
        return

    total = int(root.attrib.get("tests", 0))
    failures = int(root.attrib.get("failures", 0))
    errors = int(root.attrib.get("errors", 0))
    skipped = int(root.attrib.get("skipped", 0))
    passed = total - failures - errors - skipped
    pylint_score = get_pylint_score_from_xml(root)

    print(f"\n{color_text('Total   :', Color.BLUE)} {total}")
    print(f"{color_text('Passed  :', Color.GREEN)} {passed}")
    print(f"{color_text('Failed  :', Color.RED)} {failures}")
    print(f"{color_text('Errors  :', Color.RED)} {errors}")
    print(f"{color_text('Warnings:', Color.YELLOW)} {skipped}")
    print(f"{color_text('Score   :', Color.BLUE)} {pylint_score}")

    print(f"\n{color_text('Detailed Issues:', Color.BLUE)}\n")

    for testcase in root.findall("testcase"):
        failure = testcase.find("failure")
        if failure is None:
            continue

        file_name = testcase.attrib.get("name", "unknown")
        details = (failure.text or "").strip().splitlines()

        print(color_text(file_name, Color.RED))
        for line in details:
            parts = line.split(":", 2)
            if len(parts) >= 3:
                line_no = parts[1]
                rest = parts[2].strip()
                print(color_text(f"  - line {line_no}: {rest}", Color.RED))
            else:
                print(color_text(f"  - {line}", Color.RED))
        print()

    system_out = root.findtext("system-out", default="").strip()
    if system_out and total == 0:
        print(f"Note: {system_out}")

    print(f"\nFull pylint log: {PYLINT_LOG.relative_to(PROJECT_ROOT)}")
    print(f"\n{color_text('===================================', Color.BLUE)}\n")


def run_mypy_command(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def run_mypy(target: str | None = None) -> int:
    ensure_reports_dir()

    mypy_exe = shutil.which("mypy")
    if mypy_exe is None:
        print(f"{color_text('WARNING:', Color.YELLOW)} mypy not found in PATH.")
        create_empty_mypy_xml("mypy not installed")
        MYPY_LOG.write_text("mypy not installed\n", encoding="utf-8")
        return 0

    manual_targets = get_manual_python_target(target, "mypy")
    targets = (
        manual_targets
        if manual_targets is not None
        else get_recently_changed_python_files()
    )

    if not targets:
        print(
            f"{color_text('WARNING:', Color.YELLOW)} "
            "No recently changed Python files found by git for mypy."
        )
        create_empty_mypy_xml("no recently changed python files found")
        MYPY_LOG.write_text(
            "no recently changed python files found\n",
            encoding="utf-8",
        )
        return 0

    print(
        color_text(
            "[INFO] Running mypy on recently changed Python files:",
            Color.BLUE,
        )
    )
    for target_path in targets:
        try:
            rel = target_path.relative_to(PROJECT_ROOT)
        except ValueError:
            rel = target_path
        print(f"  - {rel}")

    cmd = [
        mypy_exe,
        "--show-error-codes",
        "--no-color-output",
        "--no-error-summary",
        *[str(path) for path in targets],
    ]
    result = run_mypy_command(cmd)

    with MYPY_LOG.open("w", encoding="utf-8") as handle:
        handle.write("STDOUT\n")
        handle.write("=" * 80 + "\n")
        handle.write(result.stdout or "")
        handle.write("\n\nSTDERR\n")
        handle.write("=" * 80 + "\n")
        handle.write(result.stderr or "")
        handle.write("\n")

    print(
        f"{color_text('[INFO]', Color.BLUE)} "
        f"Full mypy log saved to: {MYPY_LOG.relative_to(PROJECT_ROOT)}"
    )

    write_mypy_xml(
        stdout=result.stdout,
        stderr=result.stderr,
        targets=targets,
    )
    return result.returncode


def write_mypy_xml(
    stdout: str,
    stderr: str,
    targets: list[Path],
) -> None:
    commit_info = get_commit_info()

    testsuite = ET.Element("testsuite")
    testsuite.set("name", "mypy")
    testsuite.set("tests", str(len(targets)))

    failures = 0
    issues_by_file: dict[str, list[str]] = {}

    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = re.match(
            r"^(.*?\.py)(?::\d+)?(?::\d+)?:\s*(error|note):\s*(.*)$",
            line,
        )
        if match:
            file_key = match.group(1).strip()
            issues_by_file.setdefault(file_key, []).append(line)

    for target in targets:
        try:
            rel = str(target.relative_to(PROJECT_ROOT))
        except ValueError:
            rel = str(target)

        testcase = ET.SubElement(testsuite, "testcase")
        testcase.set("classname", "mypy")
        testcase.set("name", rel)

        issues = (
            issues_by_file.get(str(target), [])
            or issues_by_file.get(rel, [])
        )
        if issues:
            failures += 1
            failure = ET.SubElement(testcase, "failure")
            failure.set("message", f"{len(issues)} mypy issue(s)")
            failure.text = "\n".join(issues)

    testsuite.set("failures", str(failures))
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    system_out = ET.SubElement(testsuite, "system-out")
    system_out.text = (
        f"branch={commit_info['branch']}\n"
        f"commit={commit_info['commit']}\n"
        f"git_status={commit_info['status']}\n"
        f"{stderr.strip()}".strip()
    )

    tree = ET.ElementTree(testsuite)
    tree.write(MYPY_XML, encoding="utf-8", xml_declaration=True)


def create_empty_mypy_xml(reason: str) -> None:
    commit_info = get_commit_info()

    testsuite = ET.Element("testsuite")
    testsuite.set("name", "mypy")
    testsuite.set("tests", "0")
    testsuite.set("failures", "0")
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    ET.SubElement(testsuite, "properties")

    system_out = ET.SubElement(testsuite, "system-out")
    system_out.text = (
        f"branch={commit_info['branch']}\n"
        f"commit={commit_info['commit']}\n"
        f"git_status={commit_info['status']}\n"
        f"reason={reason}"
    )

    tree = ET.ElementTree(testsuite)
    tree.write(MYPY_XML, encoding="utf-8", xml_declaration=True)


def print_mypy_summary(commit_info: dict[str, str]) -> None:
    print(f"\n{color_text('=========== MYPY REPORT ===========', Color.BLUE)}\n")
    print(f"Branch  : {commit_info['branch']}")
    print(f"Commit  : {commit_info['commit']}")
    print(f"Git     : {commit_info['status']}")

    if not MYPY_XML.exists():
        print(f"\n{color_text('WARNING:', Color.YELLOW)} No mypy XML report found.")
        print(f"\n{color_text('===================================', Color.BLUE)}\n")
        return

    try:
        tree = ET.parse(MYPY_XML)
        root = tree.getroot()
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(
            f"\n{color_text('WARNING:', Color.YELLOW)} "
            f"Failed to parse mypy XML report: {exc}"
        )
        print(f"\n{color_text('===================================', Color.BLUE)}\n")
        return

    total = int(root.attrib.get("tests", 0))
    failures = int(root.attrib.get("failures", 0))
    errors = int(root.attrib.get("errors", 0))
    skipped = int(root.attrib.get("skipped", 0))
    passed = total - failures - errors - skipped

    print(f"\n{color_text('Total   :', Color.BLUE)} {total}")
    print(f"{color_text('Passed  :', Color.GREEN)} {passed}")
    print(f"{color_text('Failed  :', Color.RED)} {failures}")
    print(f"{color_text('Errors  :', Color.RED)} {errors}")
    print(f"{color_text('Warnings:', Color.YELLOW)} {skipped}")

    print(f"\n{color_text('Detailed Issues:', Color.BLUE)}\n")

    for testcase in root.findall("testcase"):
        failure = testcase.find("failure")
        if failure is None:
            continue

        file_name = testcase.attrib.get("name", "unknown")
        details = (failure.text or "").strip().splitlines()

        print(color_text(file_name, Color.RED))
        for line in details:
            print(color_text(f"  - {line}", Color.RED))
        print()

    system_out = root.findtext("system-out", default="").strip()
    if system_out and total == 0:
        print(f"Note: {system_out}")

    print(f"\nFull mypy log: {MYPY_LOG.relative_to(PROJECT_ROOT)}")
    print(f"\n{color_text('===================================', Color.BLUE)}\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate test reports and run analysis tools."
    )
    parser.add_argument(
        "target",
        nargs="?",
        help="Manual target file for focused analysis",
    )
    parser.add_argument(
        "--enable-allure",
        action="store_true",
        help="Enable Allure reporting in pytest_runner.py",
    )
    args = parser.parse_args()

    ensure_reports_dir()
    commit_info = get_commit_info()

    if args.target:
        print(
            f"{color_text('[INFO]', Color.BLUE)} "
            f"Manual target override enabled: {args.target}"
        )

    pytest_exit_code = run_pytest(args.target, enable_allure=args.enable_allure)
    pylint_exit_code = run_pylint(args.target)
    mypy_exit_code = run_mypy(args.target)

    pytest_results = []
    pytest_xml_files = sorted(REPORTS_DIR.glob("*.xml"))
    pytest_xml_files = [
        path
        for path in pytest_xml_files
        if path.name not in {PYLINT_XML.name, MYPY_XML.name}
    ]

    for xml_file in pytest_xml_files:
        try:
            pytest_results.append(parse_pytest_xml(xml_file))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(
                f"{color_text('WARNING:', Color.YELLOW)} "
                f"Could not parse '{xml_file}': {exc}"
            )

    print_pytest_summary(pytest_results, commit_info)
    print_pylint_summary(commit_info)
    print_mypy_summary(commit_info)

    if pytest_exit_code != 0:
        return pytest_exit_code
    if pylint_exit_code != 0:
        return pylint_exit_code
    return mypy_exit_code


if __name__ == "__main__":
    sys.exit(main())
