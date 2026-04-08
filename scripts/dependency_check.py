from __future__ import annotations

import argparse
import ast
import importlib
import shutil
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
DEPENDENCY_XML = REPORTS_DIR / "dependency-report.xml"

FIXED_TARGETS = [
    "common/log_parser/acs_info.py",
    "common/log_parser/apply_waivers.py",
    "common/log_parser/generate_acs_summary.py",
    "common/log_parser/merge_jsons.py",
    "common/log_parser/merge_summary.py",
]

MANUAL_DEPENDENCY_RULES: dict[str, dict[str, list[str]]] = {
    "acs_info.py": {
        "python_modules": [],
        "commands": [],
        "paths": [],
    },
    "apply_waivers.py": {
        "python_modules": [],
        "commands": [],
        "paths": [],
    },
    "generate_acs_summary.py": {
        "python_modules": ["jinja2"],
        "commands": ["dmidecode", "date"],
        "paths": [],
    },
    "merge_jsons.py": {
        "python_modules": [],
        "commands": [],
        "paths": [
            "/mnt/yocto_image.flag",
            "/usr/bin/log_parser/test_category.json",
            "/usr/bin/log_parser/test_categoryDT.json",
        ],
    },
    "merge_summary.py": {
        "python_modules": [],
        "commands": [],
        "paths": [],
    },
}

PATH_CHECK_FUNCTIONS = {
    "open",
    "Path",
    "isfile",
    "exists",
}

COMMAND_FUNCTIONS = {
    "run",
    "check_output",
    "Popen",
}

PATH_SUFFIXES = (
    ".json",
    ".log",
    ".txt",
    ".html",
)


def ensure_reports_dir() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_fixed_targets() -> list[Path]:
    targets: list[Path] = []

    for rel_path in FIXED_TARGETS:
        path = (PROJECT_ROOT / rel_path).resolve()
        if path.exists() and path.is_file():
            targets.append(path)
        else:
            print(f"[WARN] Target not found: {path}")

    return targets


def safe_parse_python_file(file_path: Path) -> ast.AST | None:
    try:
        source = file_path.read_text(encoding="utf-8")
        return ast.parse(source, filename=str(file_path))
    except Exception:
        return None


def get_local_python_module_names() -> set[str]:
    local_modules: set[str] = set()

    for root in (SCRIPT_DIR, PROJECT_ROOT):
        if not root.exists():
            continue
        for py_file in root.rglob("*.py"):
            if py_file.name == "__init__.py":
                continue
            local_modules.add(py_file.stem)

    return local_modules


def get_call_name(node: ast.Call) -> str | None:
    func = node.func

    if isinstance(func, ast.Name):
        return func.id

    if isinstance(func, ast.Attribute):
        return func.attr

    return None


def get_string_value(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def extract_imports(tree: ast.AST) -> set[str]:
    imports: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])

    return imports


def extract_commands(tree: ast.AST) -> set[str]:
    commands: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        call_name = get_call_name(node)
        if call_name not in COMMAND_FUNCTIONS:
            continue

        if not node.args:
            continue

        first_arg = node.args[0]

        if isinstance(first_arg, (ast.List, ast.Tuple)) and first_arg.elts:
            first_item = get_string_value(first_arg.elts[0])
            if first_item:
                commands.add(first_item)

    return commands


def looks_like_path(path_value: str) -> bool:
    if "/" in path_value:
        return True
    return path_value.endswith(PATH_SUFFIXES)


def extract_paths(tree: ast.AST) -> set[str]:
    paths: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        call_name = get_call_name(node)
        if call_name not in PATH_CHECK_FUNCTIONS:
            continue

        if not node.args:
            continue

        first_arg = node.args[0]
        path_value = get_string_value(first_arg)
        if path_value and looks_like_path(path_value):
            paths.add(path_value)

    return paths


def autodetect_file_dependencies(
    file_path: Path,
    local_modules: set[str],
) -> dict[str, list[str]]:
    tree = safe_parse_python_file(file_path)
    if tree is None:
        return {
            "python_modules": [],
            "commands": [],
            "paths": [],
        }

    stdlib_modules = set(sys.stdlib_module_names)

    imports = extract_imports(tree)
    third_party_modules = sorted(
        module_name
        for module_name in imports
        if module_name not in stdlib_modules and module_name not in local_modules
    )

    commands = sorted(extract_commands(tree))
    paths = sorted(extract_paths(tree))

    return {
        "python_modules": third_party_modules,
        "commands": commands,
        "paths": paths,
    }


def get_manual_file_dependencies(file_path: Path) -> dict[str, list[str]]:
    default_value: dict[str, list[str]] = {
        "python_modules": [],
        "commands": [],
        "paths": [],
    }

    raw_value = MANUAL_DEPENDENCY_RULES.get(file_path.name)
    if raw_value is None:
        return default_value

    return {
        "python_modules": list(raw_value["python_modules"]),
        "commands": list(raw_value["commands"]),
        "paths": list(raw_value["paths"]),
    }


def check_python_module(module_name: str) -> dict[str, str]:
    try:
        importlib.import_module(module_name)
        return {
            "name": module_name,
            "status": "OK",
            "details": "import succeeded",
        }
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {
            "name": module_name,
            "status": "MISSING",
            "details": str(exc),
        }


def check_command(command_name: str) -> dict[str, str]:
    resolved = shutil.which(command_name)
    return {
        "name": command_name,
        "status": "OK" if resolved else "MISSING",
        "details": resolved or "not found in PATH",
    }


def classify_path_expectation(path_str: str) -> tuple[str, str]:
    path = Path(path_str)

    if path_str == "/mnt/yocto_image.flag":
        return "INFO", "used to decide DT vs SR mode"

    if path_str.endswith("test_categoryDT.json"):
        if Path("/mnt/yocto_image.flag").exists():
            return (
                "OK" if path.exists() else "MISSING",
                "required in DT mode",
            )
        return "INFO", "not required in SR mode"

    if path_str.endswith("test_category.json"):
        if Path("/mnt/yocto_image.flag").exists():
            return "INFO", "not required in DT mode"
        return (
            "OK" if path.exists() else "MISSING",
            "required in SR mode",
        )

    return (
        "OK" if path.exists() else "MISSING",
        "exists" if path.exists() else "path not found",
    )


def check_path(path_str: str) -> dict[str, str]:
    status, details = classify_path_expectation(path_str)
    return {
        "name": path_str,
        "status": status,
        "details": details,
    }


def build_results_for_targets(
    targets: list[Path],
    auto_detect: bool,
) -> dict[str, dict[str, list[dict[str, str]]]]:
    local_modules = get_local_python_module_names()
    results: dict[str, dict[str, list[dict[str, str]]]] = {}

    for target in targets:
        if auto_detect:
            detected = autodetect_file_dependencies(target, local_modules)
        else:
            detected = get_manual_file_dependencies(target)

        python_results = [check_python_module(name) for name in detected["python_modules"]]
        command_results = [check_command(name) for name in detected["commands"]]
        path_results = [check_path(path_str) for path_str in detected["paths"]]

        results[str(target)] = {
            "python_modules": python_results,
            "commands": command_results,
            "paths": path_results,
        }

    return results


def write_dependency_xml(
    targets: list[Path],
    results: dict[str, dict[str, list[dict[str, str]]]],
    mode: str,
) -> None:
    testsuite = ET.Element("testsuite")
    testsuite.set("name", "dependency_check")

    total_tests = 0
    failures = 0

    properties = ET.SubElement(testsuite, "properties")

    mode_prop = ET.SubElement(properties, "property")
    mode_prop.set("name", "dependency_mode")
    mode_prop.set("value", mode)

    targets_prop = ET.SubElement(properties, "property")
    targets_prop.set("name", "dependency_targets")
    targets_prop.set("value", ",".join(str(path) for path in targets))

    for target in targets:
        target_results = results.get(str(target), {})

        for section_name in ("python_modules", "commands", "paths"):
            items = target_results.get(section_name, [])

            if not items:
                testcase = ET.SubElement(testsuite, "testcase")
                testcase.set("classname", f"dependency.{section_name}")
                testcase.set("name", f"{target.name}::none_detected")
                total_tests += 1
                continue

            for item in items:
                testcase = ET.SubElement(testsuite, "testcase")
                testcase.set("classname", f"dependency.{section_name}")
                testcase.set("name", f"{target.name}::{item['name']}")
                total_tests += 1

                if item["status"] == "MISSING":
                    failures += 1
                    failure = ET.SubElement(testcase, "failure")
                    failure.set(
                        "message",
                        f"Missing {section_name[:-1].replace('_', ' ')}: {item['name']}",
                    )
                    failure.text = item["details"]

    testsuite.set("tests", str(total_tests))
    testsuite.set("failures", str(failures))
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    system_out = ET.SubElement(testsuite, "system-out")
    system_out.text = (
        f"Dependency check mode: {mode}. "
        "Fixed targets are always used. "
        "With --auto, dependencies are autodetected from those fixed target files."
    )

    tree = ET.ElementTree(testsuite)
    tree.write(DEPENDENCY_XML, encoding="utf-8", xml_declaration=True)


def print_console_report(
    targets: list[Path],
    results: dict[str, dict[str, list[dict[str, str]]]],
    mode: str,
) -> None:
    print("========== DEPENDENCY CHECK ==========\n")
    print(f"Mode: {mode}")
    print("Target selection: fixed targets only\n")

    if not targets:
        print("No target Python files found.")
        print("\n======================================")
        return

    print("Targets:")
    for target in targets:
        try:
            rel = target.relative_to(PROJECT_ROOT)
        except ValueError:
            rel = target
        print(f"  - {rel}")

    print()

    for target in targets:
        target_results = results.get(str(target), {})

        try:
            rel = target.relative_to(PROJECT_ROOT)
        except ValueError:
            rel = target

        print(rel)

        print("  python modules checked:")
        python_items = target_results.get("python_modules", [])
        if python_items:
            for item in python_items:
                print(f"    - {item['name']} -> {item['status']} ({item['details']})")
        else:
            print("    - none")

        print("  commands checked:")
        command_items = target_results.get("commands", [])
        if command_items:
            for item in command_items:
                print(f"    - {item['name']} -> {item['status']} ({item['details']})")
        else:
            print("    - none")

        print("  paths checked:")
        path_items = target_results.get("paths", [])
        if path_items:
            for item in path_items:
                print(f"    - {item['name']} -> {item['status']} ({item['details']})")
        else:
            print("    - none")

        print()

    print("======================================")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check runtime dependencies for fixed target Python files."
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Autodetect dependencies for the fixed target files.",
    )
    args = parser.parse_args()

    ensure_reports_dir()

    mode = (
        "auto-detect dependencies for fixed targets"
        if args.auto
        else "manual rules for fixed targets"
    )
    targets = get_fixed_targets()
    results = build_results_for_targets(targets, auto_detect=args.auto)

    write_dependency_xml(targets, results, mode)
    print_console_report(targets, results, mode)

    missing_any = False
    for target_results in results.values():
        for section_name in ("python_modules", "commands", "paths"):
            for item in target_results.get(section_name, []):
                if item["status"] == "MISSING":
                    missing_any = True
                    break

    return 1 if missing_any else 0


if __name__ == "__main__":
    sys.exit(main())
