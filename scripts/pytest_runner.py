from __future__ import annotations

import argparse
import ast
import importlib.util
import os
import py_compile
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import traceback
import xml.etree.ElementTree as xml_et
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

# Optional Allure import
try:
    import allure  # type: ignore[import-not-found]
    ALLURE_AVAILABLE = True
except ImportError:
    ALLURE_AVAILABLE = False


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TEST_YAML_DIR = PROJECT_ROOT / "test_yaml"
REPORTS_DIR = PROJECT_ROOT / "reports"
PLACEHOLDER_XML = REPORTS_DIR / "pytest-placeholder.xml"
SUPPORTED_SUFFIXES = {".yaml", ".yml"}
DEFAULT_CLI_TIMEOUT_SEC = 20
DESTRUCTIVE_TEST_ENV = "RUN_DESTRUCTIVE_HW_TESTS"


class Color:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"


def color_text(text: str, color: str) -> str:
    return f"{color}{text}{Color.RESET}"


@dataclass
class TestMeta:
    suite_name: str
    phase: str
    test_type: str


@dataclass
class TestOutcome:
    testcase_name: str
    file_path: str
    passed: bool
    message: str
    meta: TestMeta
    details: str = ""
    flags: dict[str, bool] = field(
        default_factory=lambda: {
            "error": False,
            "skipped": False,
        }
    )

    @property
    def error(self) -> bool:
        return self.flags["error"]

    @property
    def skipped(self) -> bool:
        return self.flags["skipped"]


class ConfigError(Exception):
    """Raised when a YAML configuration is invalid."""


class SkipCase(Exception):
    """Raised when a case should be skipped due to environment gating."""


@dataclass
class CommandRunResult:
    command_text: str
    stdout: str
    stderr: str
    exit_code: int | None
    timed_out: bool
    timeout_sec: int
    shell_mode: bool


@dataclass(frozen=True)
class RunCaseOptions:
    suite_command: str | None = None
    allure_enabled: bool = False


def discover_yaml_files() -> list[Path]:
    if not TEST_YAML_DIR.exists() or not TEST_YAML_DIR.is_dir():
        return []
    return sorted(
        path
        for path in TEST_YAML_DIR.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
    )


def get_group_name(yaml_file: Path) -> str:
    rel_path = yaml_file.relative_to(TEST_YAML_DIR)
    return rel_path.parts[0] if len(rel_path.parts) > 1 else yaml_file.stem


def sanitize_name(value: str) -> str:
    return "".join(
        char if char.isalnum() or char in {"-", "_", "."} else "_"
        for char in value
    )


def is_valid_xml_char(code: int) -> bool:
    return (
        code in {0x9, 0xA, 0xD}
        or 0x20 <= code <= 0xD7FF
        or 0xE000 <= code <= 0xFFFD
        or 0x10000 <= code <= 0x10FFFF
    )


def sanitize_xml_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)

    cleaned: list[str] = []
    for char in value:
        if is_valid_xml_char(ord(char)):
            cleaned.append(char)
    return "".join(cleaned)


def build_report_path(group_name: str, yaml_file: Path) -> Path:
    safe_group = sanitize_name(group_name)
    safe_yaml = sanitize_name(yaml_file.stem)
    return REPORTS_DIR / f"{safe_group}__{safe_yaml}.xml"


def create_placeholder_xml(reason: str) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    testsuite = xml_et.Element("testsuite")
    testsuite.set("name", sanitize_xml_text("pytest"))
    testsuite.set("tests", "0")
    testsuite.set("failures", "0")
    testsuite.set("errors", "0")
    testsuite.set("skipped", "0")

    properties = xml_et.SubElement(testsuite, "properties")

    reason_prop = xml_et.SubElement(properties, "property")
    reason_prop.set("name", "reason")
    reason_prop.set("value", sanitize_xml_text(reason))

    placeholder_prop = xml_et.SubElement(properties, "property")
    placeholder_prop.set("name", "placeholder")
    placeholder_prop.set("value", "true")

    system_out = xml_et.SubElement(testsuite, "system-out")
    system_out.text = sanitize_xml_text(reason)

    xml_et.ElementTree(testsuite).write(
        PLACEHOLDER_XML,
        encoding="utf-8",
        xml_declaration=True,
    )


def remove_placeholder_xml() -> None:
    if PLACEHOLDER_XML.exists():
        PLACEHOLDER_XML.unlink()


def cleanup_old_pytest_xml_reports() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    for xml_file in REPORTS_DIR.glob("*.xml"):
        if xml_file.name == "pylint-report.xml":
            continue
        xml_file.unlink(missing_ok=True)


def load_yaml_config(yaml_file: Path) -> dict[str, Any]:
    try:
        with yaml_file.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError("Top-level YAML content must be a mapping")

    return data


def ensure_list(value: Any, field_name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ConfigError(f"'{field_name}' must be a list")
    return value


def ensure_list_of_strings(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"'{field_name}' must be a list of strings")
    return value


def ensure_string_or_list_of_strings(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return ensure_list_of_strings(value, field_name)


def normalize_suite_files(files_value: Any, field_name: str) -> list[str]:
    items = ensure_list(files_value, field_name)
    if not items:
        raise ConfigError(f"'{field_name}' must not be empty")

    normalized: list[str] = []
    for item in items:
        if isinstance(item, str):
            normalized.append(item)
            continue
        if isinstance(item, dict) and isinstance(item.get("path"), str):
            normalized.append(item["path"])
            continue
        raise ConfigError(
            f"Each entry in '{field_name}' must be either a string or {{path: ...}}"
        )
    return normalized


def validate_post_checks(post_checks: Any, field_name: str) -> None:
    if post_checks is None:
        return

    checks = ensure_list(post_checks, field_name)
    supported = {
        "exists",
        "not_exists",
        "file_contains",
        "file_not_contains",
        "file_not_empty",
        "regex",
        "ordered_contains",
    }

    for index, check in enumerate(checks, start=1):
        entry_name = f"{field_name}[{index}]"
        if not isinstance(check, dict):
            raise ConfigError(f"{entry_name} must be a mapping")

        check_type = check.get("type")
        if not isinstance(check_type, str):
            raise ConfigError(f"{entry_name}.type must be a string")

        if check_type not in supported:
            raise ConfigError(
                f"{entry_name}.type unsupported: {check_type}. "
                f"Supported types: {', '.join(sorted(supported))}"
            )

        raw_path = check.get("path")
        if not isinstance(raw_path, str):
            raise ConfigError(f"{entry_name}.path must be a string")

        if check_type in {"file_contains", "file_not_contains"}:
            text = check.get("text")
            if not isinstance(text, str):
                raise ConfigError(f"{entry_name}.text must be a string")

        if check_type == "regex":
            pattern = check.get("pattern")
            if not isinstance(pattern, str):
                raise ConfigError(f"{entry_name}.pattern must be a string")

        if check_type == "ordered_contains":
            texts = check.get("texts")
            if not isinstance(texts, list) or not all(
                isinstance(item, str) for item in texts
            ):
                raise ConfigError(f"{entry_name}.texts must be a list of strings")


def validate_env_mapping(
    value: Any,
    field_name: str,
    *,
    allow_bool_values: bool = False,
) -> None:
    if not isinstance(value, dict):
        raise ConfigError(f"{field_name} must be a mapping")

    valid_value_types = (str, bool) if allow_bool_values else (str,)
    for key, item in value.items():
        if not isinstance(key, str):
            raise ConfigError(f"{field_name} keys must be strings")
        if not isinstance(item, valid_value_types):
            raise ConfigError(
                f"{field_name}['{key}'] must be "
                f"{'string/bool' if allow_bool_values else 'a string'}"
            )


def validate_command_probe_list(value: Any, field_name: str) -> None:
    probes = ensure_list(value, field_name)
    for index, probe in enumerate(probes, start=1):
        entry_name = f"{field_name}[{index}]"
        if isinstance(probe, str):
            continue
        if not isinstance(probe, dict):
            raise ConfigError(f"{entry_name} must be a string or mapping")

        command = probe.get("command")
        if not isinstance(command, str):
            raise ConfigError(f"{entry_name}.command must be a string")

        args = probe.get("args")
        if args is not None and (
            not isinstance(args, list) or not all(isinstance(item, str) for item in args)
        ):
            raise ConfigError(f"{entry_name}.args must be a list of strings")

        shell_value = probe.get("shell")
        if shell_value is not None and not isinstance(shell_value, bool):
            raise ConfigError(f"{entry_name}.shell must be a boolean")

        timeout_sec = probe.get("timeout_sec")
        if timeout_sec is not None and (
            not isinstance(timeout_sec, int) or timeout_sec <= 0
        ):
            raise ConfigError(f"{entry_name}.timeout_sec must be a positive integer")

        cwd = probe.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise ConfigError(f"{entry_name}.cwd must be a string")

        env = probe.get("env")
        if env is not None:
            validate_env_mapping(env, f"{entry_name}.env")


def validate_common_case_controls(case_def: dict[str, Any], field_name: str) -> None:
    skip_unless_env = case_def.get("skip_unless_env")
    if skip_unless_env is not None:
        validate_env_mapping(
            skip_unless_env,
            f"{field_name}.skip_unless_env",
            allow_bool_values=True,
        )

    skip_unless_paths_exist = case_def.get("skip_unless_paths_exist")
    if skip_unless_paths_exist is not None:
        ensure_string_or_list_of_strings(
            skip_unless_paths_exist,
            f"{field_name}.skip_unless_paths_exist",
        )

    skip_unless_commands_succeed = case_def.get("skip_unless_commands_succeed")
    if skip_unless_commands_succeed is not None:
        validate_command_probe_list(
            skip_unless_commands_succeed,
            f"{field_name}.skip_unless_commands_succeed",
        )

    requires_destructive = case_def.get("requires_destructive")
    if requires_destructive is not None and not isinstance(requires_destructive, bool):
        raise ConfigError(f"{field_name}.requires_destructive must be a boolean")

    required_env = case_def.get("required_env")
    if required_env is not None:
        validate_env_mapping(required_env, f"{field_name}.required_env")


def validate_case_schema(case_def: dict[str, Any], field_name: str) -> None:
    case_name = case_def.get("name")
    if not isinstance(case_name, str) or not case_name.strip():
        raise ConfigError(f"{field_name}.name must be a non-empty string")

    case_type = case_def.get("type", "cli")
    if not isinstance(case_type, str):
        raise ConfigError(f"{field_name}.type must be a string")

    if case_type not in {
        "file_exists",
        "py_compile",
        "source_contains",
        "source_contains_any",
        "source_contains_all",
        "function_exists",
        "function_exists_any",
        "main_guard",
        "cli",
        "py_function",
        "module_main_with_env",
        "path_exists",
        "path_not_exists",
        "command_success",
        "command_exit_code",
        "command_output_contains",
        "command_output_regex",
    }:
        raise ConfigError(f"{field_name}.type unsupported: {case_type}")

    validate_common_case_controls(case_def, field_name)

    if "scripts" in case_def and case_def["scripts"] is not None:
        scripts = case_def["scripts"]
        if not isinstance(scripts, dict):
            raise ConfigError(f"{field_name}.scripts must be a mapping")
        for key, value in scripts.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ConfigError(
                    f"{field_name}.scripts entries must be string -> string"
                )

    if "bin_files" in case_def and case_def["bin_files"] is not None:
        bin_files = case_def["bin_files"]
        if not isinstance(bin_files, dict):
            raise ConfigError(f"{field_name}.bin_files must be a mapping")
        for key, spec in bin_files.items():
            if not isinstance(key, str) or not isinstance(spec, dict):
                raise ConfigError(
                    f"{field_name}.bin_files entries must be string -> mapping"
                )
            hex_data = spec.get("hex")
            text_data = spec.get("text")
            if hex_data is None and text_data is None:
                raise ConfigError(
                    f"{field_name}.bin_files['{key}'] requires 'hex' or 'text'"
                )
            if hex_data is not None and not isinstance(hex_data, str):
                raise ConfigError(
                    f"{field_name}.bin_files['{key}'].hex must be a string"
                )
            if text_data is not None and not isinstance(text_data, str):
                raise ConfigError(
                    f"{field_name}.bin_files['{key}'].text must be a string"
                )

    if "text_files" in case_def and case_def["text_files"] is not None:
        text_files = case_def["text_files"]
        if not isinstance(text_files, dict):
            raise ConfigError(f"{field_name}.text_files must be a mapping")
        for key, value in text_files.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ConfigError(
                    f"{field_name}.text_files entries must be string -> string"
                )

    if "patch_constants" in case_def and case_def["patch_constants"] is not None:
        patch_constants = case_def["patch_constants"]
        if not isinstance(patch_constants, dict):
            raise ConfigError(f"{field_name}.patch_constants must be a mapping")
        for key, value in patch_constants.items():
            if not isinstance(key, str):
                raise ConfigError(
                    f"{field_name}.patch_constants keys must be strings"
                )
            if not isinstance(value, (str, int, float, bool)):
                raise ConfigError(
                    f"{field_name}.patch_constants['{key}'] "
                    "must be a scalar string/int/float/bool"
                )

    if "dir_structure" in case_def and case_def["dir_structure"] is not None:
        dir_structure = case_def["dir_structure"]
        if not isinstance(dir_structure, list):
            raise ConfigError(f"{field_name}.dir_structure must be a list")
        for index, entry in enumerate(dir_structure, start=1):
            if not isinstance(entry, dict):
                raise ConfigError(
                    f"{field_name}.dir_structure[{index}] must be a mapping"
                )
            if not isinstance(entry.get("path"), str):
                raise ConfigError(
                    f"{field_name}.dir_structure[{index}].path must be a string"
                )

    validate_post_checks(case_def.get("post_checks"), f"{field_name}.post_checks")

    if case_type == "py_function":
        function_name = case_def.get("function")
        if not isinstance(function_name, str) or not function_name.strip():
            raise ConfigError(f"{field_name}.function must be a non-empty string")

        args = case_def.get("args", [])
        if not isinstance(args, list):
            raise ConfigError(f"{field_name}.args must be a list")

        kwargs = case_def.get("kwargs")
        if kwargs is not None and not isinstance(kwargs, dict):
            raise ConfigError(f"{field_name}.kwargs must be a mapping")

        if "expect_exception" in case_def and not isinstance(
            case_def["expect_exception"], str
        ):
            raise ConfigError(f"{field_name}.expect_exception must be a string")

        if "expect_return_contains" in case_def and not isinstance(
            case_def["expect_return_contains"], str
        ):
            raise ConfigError(f"{field_name}.expect_return_contains must be a string")

        return

    if case_type == "module_main_with_env":
        expected_exit_code = case_def.get("expect_exit_code")
        if expected_exit_code is not None and not isinstance(expected_exit_code, int):
            raise ConfigError(f"{field_name}.expect_exit_code must be an integer")
        return

    if case_type in {"path_exists", "path_not_exists"}:
        path_value = case_def.get("path")
        if not isinstance(path_value, str):
            raise ConfigError(f"{field_name}.path must be a string")
        return

    if case_type in {
        "command_success",
        "command_exit_code",
        "command_output_contains",
        "command_output_regex",
    }:
        command = case_def.get("command")
        if not isinstance(command, str):
            raise ConfigError(f"{field_name}.command must be a string")

        args = case_def.get("args", [])
        if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
            raise ConfigError(f"{field_name}.args must be a list of strings")

        shell_value = case_def.get("shell", False)
        if not isinstance(shell_value, bool):
            raise ConfigError(f"{field_name}.shell must be a boolean")

        timeout_sec = case_def.get("timeout_sec", DEFAULT_CLI_TIMEOUT_SEC)
        if not isinstance(timeout_sec, int) or timeout_sec <= 0:
            raise ConfigError(f"{field_name}.timeout_sec must be a positive integer")

        cwd = case_def.get("cwd")
        if cwd is not None and not isinstance(cwd, str):
            raise ConfigError(f"{field_name}.cwd must be a string")

        env = case_def.get("env")
        if env is not None:
            validate_env_mapping(env, f"{field_name}.env")

        if case_type == "command_exit_code":
            expected_exit_code = case_def.get("expect_exit_code")
            if not isinstance(expected_exit_code, int):
                raise ConfigError(f"{field_name}.expect_exit_code must be an integer")

        if case_type == "command_output_contains":
            expect_text = case_def.get("expect_text")
            if not isinstance(expect_text, str):
                raise ConfigError(f"{field_name}.expect_text must be a string")

        if case_type == "command_output_regex":
            expect_pattern = case_def.get("expect_pattern")
            if not isinstance(expect_pattern, str):
                raise ConfigError(f"{field_name}.expect_pattern must be a string")
        return

    if case_type != "cli":
        return

    raw_args = case_def.get("args", [])
    if not isinstance(raw_args, list) or not all(
        isinstance(arg, str) for arg in raw_args
    ):
        raise ConfigError(f"{field_name}.args must be a list of strings")

    command = case_def.get("command")
    if command is not None and not isinstance(command, str):
        raise ConfigError(f"{field_name}.command must be a string")

    stdin_text = case_def.get("stdin")
    if stdin_text is not None and not isinstance(stdin_text, str):
        raise ConfigError(f"{field_name}.stdin must be a string")

    timeout_sec = case_def.get("timeout_sec", DEFAULT_CLI_TIMEOUT_SEC)
    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ConfigError(f"{field_name}.timeout_sec must be a positive integer")

    shell_value = case_def.get("shell", False)
    if not isinstance(shell_value, bool):
        raise ConfigError(f"{field_name}.shell must be a boolean")

    expect_timeout = case_def.get("expect_timeout", False)
    if not isinstance(expect_timeout, bool):
        raise ConfigError(f"{field_name}.expect_timeout must be a boolean")

    env = case_def.get("env")
    if env is not None:
        validate_env_mapping(env, f"{field_name}.env")

    expected_exit_code = case_def.get("expect_exit_code")
    if expected_exit_code is not None and not isinstance(expected_exit_code, int):
        raise ConfigError(f"{field_name}.expect_exit_code must be an integer")

    expect_exit_nonzero = case_def.get("expect_exit_nonzero", False)
    if not isinstance(expect_exit_nonzero, bool):
        raise ConfigError(f"{field_name}.expect_exit_nonzero must be a boolean")

    expect_exit_code_in = case_def.get("expect_exit_code_in")
    if expect_exit_code_in is not None:
        if not isinstance(expect_exit_code_in, list) or not all(
            isinstance(item, int) for item in expect_exit_code_in
        ):
            raise ConfigError(
                f"{field_name}.expect_exit_code_in must be a list of integers"
            )

    expect_stdout_or_stderr_regex = case_def.get("expect_stdout_or_stderr_regex")
    if expect_stdout_or_stderr_regex is not None:
        if isinstance(expect_stdout_or_stderr_regex, str):
            pass
        elif not isinstance(expect_stdout_or_stderr_regex, list) or not all(
            isinstance(item, str) for item in expect_stdout_or_stderr_regex
        ):
            raise ConfigError(
                f"{field_name}.expect_stdout_or_stderr_regex must be a string or list of strings"
            )


def normalize_cases(value: Any, field_name: str) -> list[dict[str, Any]]:
    items = ensure_list(value, field_name)
    normalized: list[dict[str, Any]] = []

    for index, case_def in enumerate(items, start=1):
        if not isinstance(case_def, dict):
            raise ConfigError(f"{field_name}[{index}] must be a mapping")
        validate_case_schema(case_def, f"{field_name}[{index}]")
        normalized.append(case_def)

    return normalized


def normalize_suites(config: dict[str, Any]) -> list[dict[str, Any]]:
    raw_suites = config.get("suites")
    if raw_suites is None:
        raise ConfigError("Top-level key 'suites' is required")

    suites = ensure_list(raw_suites, "suites")
    if not suites:
        raise ConfigError("'suites' must not be empty")

    normalized: list[dict[str, Any]] = []
    for index, suite in enumerate(suites, start=1):
        if not isinstance(suite, dict):
            raise ConfigError(f"suites[{index}] must be a mapping")

        name = suite.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"suites[{index}] requires a non-empty string 'name'")

        suite_command = suite.get("command")
        if suite_command is not None and not isinstance(suite_command, str):
            raise ConfigError(f"suites[{index}].command must be a string")

        files = normalize_suite_files(suite.get("files"), f"suites[{index}].files")
        cases = normalize_cases(suite.get("cases", []), f"suites[{index}].cases")

        normalized.append(
            {
                "name": name.strip(),
                "command": suite_command,
                "files": files,
                "cases": cases,
            }
        )

    return normalized


def resolve_target_path(file_entry: str) -> Path:
    raw = Path(file_entry)
    if raw.is_absolute():
        return raw.resolve()
    return (PROJECT_ROOT / raw).resolve()


def read_source(file_path: Path) -> str:
    return file_path.read_text(encoding="utf-8")


def parse_ast(file_path: Path) -> ast.AST:
    return ast.parse(read_source(file_path), filename=str(file_path))


def collect_function_names(file_path: Path) -> set[str]:
    tree = parse_ast(file_path)
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)

    return names


def format_outcome_message(prefix: str, text: str) -> str:
    text = text.strip()
    return prefix if not text else f"{prefix}: {text}"


def create_outcome(
    testcase_name: str,
    file_path: str,
    passed: bool,
    message: str,
    meta: TestMeta,
    **kwargs: Any,
) -> TestOutcome:
    return TestOutcome(
        testcase_name=testcase_name,
        file_path=file_path,
        passed=passed,
        message=message,
        meta=meta,
        details=kwargs.get("details", ""),
        flags={
            "error": kwargs.get("error", False),
            "skipped": kwargs.get("skipped", False),
        },
    )


def expand_template(value: str, work_dir: Path, file_path: Path) -> str:
    return value.format(dir=str(work_dir), file=str(file_path), filename=file_path.name)


def replace_dir_tokens(value: Any, temp_dir: Path) -> Any:
    if isinstance(value, str):
        return value.replace("{dir}", str(temp_dir))
    if isinstance(value, list):
        return [replace_dir_tokens(item, temp_dir) for item in value]
    if isinstance(value, dict):
        return {key: replace_dir_tokens(item, temp_dir) for key, item in value.items()}
    return value


def prepare_case_files(work_dir: Path, case_def: dict[str, Any]) -> None:
    scripts = case_def.get("scripts", {})
    if scripts is not None:
        if not isinstance(scripts, dict):
            raise ConfigError("'scripts' must be a mapping")
        for name, content in scripts.items():
            if not isinstance(name, str) or not isinstance(content, str):
                raise ConfigError("'scripts' entries must be string -> string")
            script_path = work_dir / name
            script_path.parent.mkdir(parents=True, exist_ok=True)
            script_path.write_text(content, encoding="utf-8")
            script_path.chmod(0o755)

    bin_files = case_def.get("bin_files", {})
    if bin_files is not None:
        if not isinstance(bin_files, dict):
            raise ConfigError("'bin_files' must be a mapping")
        for name, spec in bin_files.items():
            if not isinstance(name, str) or not isinstance(spec, dict):
                raise ConfigError("'bin_files' entries must be string -> mapping")

            target_file = work_dir / name
            target_file.parent.mkdir(parents=True, exist_ok=True)

            hex_data = spec.get("hex")
            text_data = spec.get("text")
            if hex_data is not None:
                if not isinstance(hex_data, str):
                    raise ConfigError(
                        f"Invalid hex data type for bin_files entry '{name}'"
                    )
                try:
                    target_file.write_bytes(bytes.fromhex(hex_data))
                except ValueError as exc:
                    raise ConfigError(
                        f"Invalid hex data for bin_files entry '{name}': {exc}"
                    ) from exc
            elif text_data is not None:
                if not isinstance(text_data, str):
                    raise ConfigError(
                        f"Invalid text data type for bin_files entry '{name}'"
                    )
                target_file.write_bytes(text_data.encode("utf-8"))
            else:
                raise ConfigError(
                    "Each 'bin_files' entry requires string key 'hex' or 'text'"
                )

    text_files = case_def.get("text_files", {})
    if text_files is not None:
        if not isinstance(text_files, dict):
            raise ConfigError("'text_files' must be a mapping")
        for name, content in text_files.items():
            if not isinstance(name, str) or not isinstance(content, str):
                raise ConfigError("'text_files' entries must be string -> string")
            target_file = work_dir / name
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_text(content, encoding="utf-8")


def render_post_check_path(raw_path: str, work_dir: Path) -> Path:
    return Path(raw_path.format(dir=str(work_dir)))


def run_post_checks(work_dir: Path, post_checks: Any) -> tuple[bool, list[str]]:
    if post_checks is None:
        return True, []

    checks = ensure_list(post_checks, "post_checks")
    messages: list[str] = []
    all_passed = True

    for index, check in enumerate(checks, start=1):
        if not isinstance(check, dict):
            raise ConfigError(f"post_checks[{index}] must be a mapping")

        check_type = check.get("type")
        raw_path = check.get("path")

        if not isinstance(check_type, str):
            raise ConfigError(f"post_checks[{index}] requires string key 'type'")
        if not isinstance(raw_path, str):
            raise ConfigError(f"post_checks[{index}] requires string key 'path'")

        resolved = render_post_check_path(raw_path, work_dir)

        if check_type == "exists":
            exists = resolved.exists()
            messages.append(
                f"post_check exists: {resolved} -> {'PASS' if exists else 'FAIL'}"
            )
            if not exists:
                all_passed = False
            continue

        if check_type == "not_exists":
            missing = not resolved.exists()
            messages.append(
                f"post_check not_exists: {resolved} -> "
                f"{'PASS' if missing else 'FAIL'}"
            )
            if not missing:
                all_passed = False
            continue

        if check_type == "file_not_empty":
            passed = (
                resolved.exists()
                and resolved.is_file()
                and resolved.stat().st_size > 0
            )
            messages.append(
                f"post_check file_not_empty: {resolved} -> "
                f"{'PASS' if passed else 'FAIL'}"
            )
            if not passed:
                all_passed = False
            continue

        if check_type in {"file_contains", "file_not_contains"}:
            text = check.get("text")
            if not isinstance(text, str):
                raise ConfigError(f"post_checks[{index}] requires string key 'text'")

            if not resolved.exists() or not resolved.is_file():
                messages.append(
                    f"post_check {check_type}: {resolved} contains {text!r} -> FAIL"
                )
                all_passed = False
                continue

            contents = resolved.read_text(encoding="utf-8", errors="replace")
            contains = text in contents
            passed = contains if check_type == "file_contains" else not contains

            messages.append(
                f"post_check {check_type}: {resolved} contains {text!r} -> "
                f"{'PASS' if passed else 'FAIL'}"
            )
            if not passed:
                all_passed = False
            continue

        if check_type == "regex":
            pattern = check.get("pattern")
            if not isinstance(pattern, str):
                raise ConfigError(
                    f"post_checks[{index}] requires string key 'pattern'"
                )

            if not resolved.exists() or not resolved.is_file():
                messages.append(
                    f"post_check regex: {resolved} matches {pattern!r} -> FAIL"
                )
                all_passed = False
                continue

            contents = resolved.read_text(encoding="utf-8", errors="replace")
            passed = re.search(pattern, contents, flags=re.MULTILINE) is not None
            messages.append(
                f"post_check regex: {resolved} matches {pattern!r} -> "
                f"{'PASS' if passed else 'FAIL'}"
            )
            if not passed:
                all_passed = False
            continue

        if check_type == "ordered_contains":
            texts = check.get("texts")
            if not isinstance(texts, list) or not all(
                isinstance(item, str) for item in texts
            ):
                raise ConfigError(
                    f"post_checks[{index}] requires key 'texts' as list[str]"
                )

            if not resolved.exists() or not resolved.is_file():
                messages.append(f"post_check ordered_contains: {resolved} -> FAIL")
                all_passed = False
                continue

            contents = resolved.read_text(encoding="utf-8", errors="replace")
            start = 0
            passed = True
            for text in texts:
                idx = contents.find(text, start)
                if idx == -1:
                    passed = False
                    messages.append(
                        f"post_check ordered_contains: {resolved} "
                        f"missing {text!r} in order -> FAIL"
                    )
                    all_passed = False
                    break
                start = idx + len(text)

            if passed:
                messages.append(f"post_check ordered_contains: {resolved} -> PASS")
            continue

        raise ConfigError(f"post_checks[{index}] unsupported type: {check_type}")

    return all_passed, messages


def load_module_from_path(file_path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        f"runner_module_{sanitize_name(file_path.stem)}",
        str(file_path),
    )
    if spec is None or spec.loader is None:
        raise ConfigError(f"Could not load module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def normalize_completed_stream(stream: Any) -> str:
    if stream is None:
        return ""
    if isinstance(stream, str):
        return stream
    if isinstance(stream, bytes):
        return stream.decode("utf-8", errors="replace")
    return str(stream)


def build_command_from_spec(
    file_path: Path,
    spec: dict[str, Any],
    work_dir: Path,
) -> tuple[list[str] | str, bool]:
    raw_args = spec.get("args", [])
    if not isinstance(raw_args, list) or not all(isinstance(arg, str) for arg in raw_args):
        raise ConfigError("'args' must be a list of strings")

    command_value = spec.get("command")
    if not isinstance(command_value, str):
        raise ConfigError("'command' must be a string")

    shell_mode = bool(spec.get("shell", False))
    expanded_command = expand_template(command_value, work_dir, file_path)
    expanded_args = [expand_template(arg, work_dir, file_path) for arg in raw_args]

    if shell_mode:
        parts = [expanded_command, *[shlex.quote(arg) for arg in expanded_args]]
        return " ".join(parts), True

    return [expanded_command, *expanded_args], False


def build_runtime_env(
    file_path: Path,
    env_spec: Any,
    work_dir: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    if env_spec is None:
        return env

    if not isinstance(env_spec, dict):
        raise ConfigError("'env' must be a mapping")

    for key, value in env_spec.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConfigError("'env' entries must be string -> string")
        env[key] = expand_template(value, work_dir, file_path)

    return env


def execute_command_spec(
    file_path: Path,
    spec: dict[str, Any],
    work_dir: Path,
    stdin_text: str | None = None,
) -> CommandRunResult:
    timeout_sec = spec.get("timeout_sec", DEFAULT_CLI_TIMEOUT_SEC)
    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ConfigError("'timeout_sec' must be a positive integer")

    cmd, shell_mode = build_command_from_spec(file_path, spec, work_dir)
    run_env = build_runtime_env(file_path, spec.get("env"), work_dir)

    cwd_value = spec.get("cwd")
    if cwd_value is None:
        cwd = work_dir
    else:
        if not isinstance(cwd_value, str):
            raise ConfigError("'cwd' must be a string")
        cwd = Path(expand_template(cwd_value, work_dir, file_path))

    timed_out = False
    stdout = ""
    stderr = ""
    exit_code: int | None = None

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            input=stdin_text,
            check=False,
            env=run_env,
            timeout=timeout_sec,
            shell=shell_mode,
        )
        stdout = normalize_completed_stream(completed.stdout)
        stderr = normalize_completed_stream(completed.stderr)
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = normalize_completed_stream(exc.stdout)
        stderr = normalize_completed_stream(exc.stderr)

    command_text = cmd if isinstance(cmd, str) else " ".join(cmd)
    return CommandRunResult(
        command_text=command_text,
        stdout=stdout,
        stderr=stderr,
        exit_code=exit_code,
        timed_out=timed_out,
        timeout_sec=timeout_sec,
        shell_mode=shell_mode,
    )


def normalize_probe_spec(probe: Any) -> dict[str, Any]:
    if isinstance(probe, str):
        return {
            "command": probe,
            "args": [],
            "shell": True,
            "timeout_sec": DEFAULT_CLI_TIMEOUT_SEC,
        }
    if isinstance(probe, dict):
        return dict(probe)
    raise ConfigError("Command probe must be a string or mapping")


def apply_skip_controls(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> None:
    skip_unless_env = case_def.get("skip_unless_env", {})
    if skip_unless_env:
        if not isinstance(skip_unless_env, dict):
            raise ConfigError("'skip_unless_env' must be a mapping")
        for key, expected in skip_unless_env.items():
            current = os.environ.get(key)
            if isinstance(expected, bool):
                present = bool(current)
                if present != expected:
                    raise SkipCase(
                        f"[HW WARNING] Env presence mismatch for {key!r}: "
                        f"got {present}, expected {expected}"
                    )
                continue
            if current != expected:
                raise SkipCase(
                    f"[HW WARNING] Env mismatch for {key!r}: "
                    f"got {current!r}, expected {expected!r}"
                )

    skip_unless_paths_exist = case_def.get("skip_unless_paths_exist", [])
    for raw_path in ensure_string_or_list_of_strings(
        skip_unless_paths_exist,
        "skip_unless_paths_exist",
    ):
        resolved = Path(expand_template(raw_path, work_dir, file_path))
        if not resolved.exists():
            raise SkipCase(f"[HW WARNING] Missing required path: {resolved}")

    skip_unless_commands_succeed = case_def.get("skip_unless_commands_succeed", [])
    probes = ensure_list(skip_unless_commands_succeed, "skip_unless_commands_succeed")
    for probe in probes:
        probe_spec = normalize_probe_spec(probe)
        result = execute_command_spec(file_path, probe_spec, work_dir)
        if result.timed_out:
            raise SkipCase(
                f"[HW WARNING] Probe command timed out after {result.timeout_sec}s: "
                f"{result.command_text}"
            )
        if result.exit_code != 0:
            raise SkipCase(
                f"[HW WARNING] Probe command failed with exit code "
                f"{result.exit_code}: {result.command_text}"
            )

    if case_def.get("requires_destructive", False):
        if os.environ.get(DESTRUCTIVE_TEST_ENV) != "1":
            raise SkipCase(
                f"[HW WARNING] Destructive test skipped. Set "
                f"{DESTRUCTIVE_TEST_ENV}=1 to enable."
            )

    required_env = case_def.get("required_env", {})
    if required_env:
        if not isinstance(required_env, dict):
            raise ConfigError("'required_env' must be a mapping")
        for key, expected in required_env.items():
            current = os.environ.get(key)
            if current != expected:
                raise ConfigError(
                    f"required_env mismatch for {key!r}: "
                    f"got {current!r}, expected {expected!r}"
                )


def check_file_exists(
    file_path: Path,
    _case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    exists = file_path.exists()
    message = "File exists" if exists else "File does not exist"
    return exists, message, "", False


def check_py_compile(
    file_path: Path,
    _case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    try:
        py_compile.compile(str(file_path), doraise=True)
        return True, "Python compilation succeeded", "", False
    except py_compile.PyCompileError as exc:
        return False, "Python compilation failed", str(exc), False


def check_source_contains(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    pattern = case_def.get("pattern")
    if not isinstance(pattern, str):
        raise ConfigError("'source_contains' requires a string 'pattern'")

    source = read_source(file_path)
    passed = pattern in source
    message = f"Found pattern: {pattern}" if passed else f"Pattern not found: {pattern}"
    return passed, message, "", False


def check_source_contains_any(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    patterns = ensure_list_of_strings(case_def.get("patterns"), "patterns")
    source = read_source(file_path)
    matched = [pattern for pattern in patterns if pattern in source]
    passed = bool(matched)
    details = "Matched patterns: " + ", ".join(matched) if matched else ""
    message = "At least one pattern matched" if passed else "No patterns matched"
    return passed, message, details, False


def check_source_contains_all(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    patterns = ensure_list_of_strings(case_def.get("patterns"), "patterns")
    source = read_source(file_path)
    missing = [pattern for pattern in patterns if pattern not in source]
    passed = not missing
    details = "Missing patterns: " + ", ".join(missing) if missing else ""
    message = "All patterns matched" if passed else "Some patterns were not found"
    return passed, message, details, False


def check_function_exists(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    function_name = case_def.get("function")
    if not isinstance(function_name, str):
        raise ConfigError("'function_exists' requires a string 'function'")

    names = collect_function_names(file_path)
    passed = function_name in names
    message = (
        f"Function found: {function_name}"
        if passed
        else f"Function not found: {function_name}"
    )
    return passed, message, "", False


def check_function_exists_any(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    functions = ensure_list_of_strings(case_def.get("functions"), "functions")
    names = collect_function_names(file_path)
    matched = [name for name in functions if name in names]
    passed = bool(matched)
    details = "Matched functions: " + ", ".join(matched) if matched else ""
    message = "At least one function matched" if passed else "No functions matched"
    return passed, message, details, False


def check_main_guard(
    file_path: Path,
    _case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    source = read_source(file_path)
    patterns = [
        'if __name__ == "__main__":',
        "if __name__ == '__main__':",
    ]
    passed = any(pattern in source for pattern in patterns)
    message = "Main guard found" if passed else "Main guard not found"
    return passed, message, "", False


def check_py_function(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    module = load_module_from_path(file_path)

    function_name = case_def.get("function")
    if not isinstance(function_name, str):
        raise ConfigError("'py_function' requires a string 'function'")

    if not hasattr(module, function_name):
        return False, f"Function not found: {function_name}", "", False

    func = getattr(module, function_name)
    args = case_def.get("args", [])
    kwargs = case_def.get("kwargs", {})

    if not isinstance(args, list):
        raise ConfigError("'py_function.args' must be a list")
    if not isinstance(kwargs, dict):
        raise ConfigError("'py_function.kwargs' must be a mapping")

    expected_exception = case_def.get("expect_exception")

    passed = True
    message = ""
    details = ""
    is_error = False

    try:
        result = func(*args, **kwargs)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if expected_exception:
            passed = exc.__class__.__name__ == expected_exception
            message = (
                f"Raised expected exception: {expected_exception}"
                if passed
                else f"Expected {expected_exception}, got {exc.__class__.__name__}: {exc}"
            )
            details = traceback.format_exc()
            is_error = not passed
        else:
            passed = False
            message = f"Unexpected exception: {exc}"
            details = traceback.format_exc()
            is_error = True
        return passed, message, details, is_error

    if expected_exception:
        return (
            False,
            f"Expected exception {expected_exception}, but function returned",
            repr(result),
            False,
        )

    expected_return = case_def.get("expect_return")
    if "expect_return" in case_def and result != expected_return:
        passed = False
        message = f"Expected return {expected_return!r}, got {result!r}"

    expected_fragment = case_def.get("expect_return_contains")
    if expected_fragment is not None and expected_fragment not in str(result):
        passed = False
        message = f"Expected return to contain {expected_fragment!r}, got {result!r}"

    if passed:
        message = f"Function returned {result!r}"

    return passed, message, details, is_error


def check_module_main_with_env(
    file_path: Path,
    case_def: dict[str, Any],
    _work_dir: Path,
) -> tuple[bool, str, str, bool]:
    temp_dir = Path(tempfile.mkdtemp(prefix="runner_env_"))
    details_lines: list[str] = [f"Temp dir: {temp_dir}"]

    try:
        runtime_case = dict(case_def)
        runtime_case["scripts"] = replace_dir_tokens(
            runtime_case.get("scripts", {}),
            temp_dir,
        )
        runtime_case["bin_files"] = replace_dir_tokens(
            runtime_case.get("bin_files", {}),
            temp_dir,
        )
        runtime_case["text_files"] = replace_dir_tokens(
            runtime_case.get("text_files", {}),
            temp_dir,
        )

        dir_structure = replace_dir_tokens(
            runtime_case.get("dir_structure", []),
            temp_dir,
        )
        for entry in dir_structure:
            rel_path = entry["path"]
            dir_path = temp_dir / rel_path
            dir_path.mkdir(parents=True, exist_ok=True)
            details_lines.append(f"Created directory: {dir_path}")

        prepare_case_files(temp_dir, runtime_case)

        module = load_module_from_path(file_path)

        patch_constants = replace_dir_tokens(
            runtime_case.get("patch_constants", {}),
            temp_dir,
        )
        for attr_name, attr_value in patch_constants.items():
            setattr(module, attr_name, attr_value)
            details_lines.append(f"Patched constant: {attr_name}={attr_value!r}")

        try:
            exit_code = module.main()
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 0

        details_lines.append(f"Exit code: {exit_code}")

        expected_exit_code = runtime_case.get("expect_exit_code")
        if expected_exit_code is not None and exit_code != expected_exit_code:
            return (
                False,
                f"Expected exit code {expected_exit_code}, got {exit_code}",
                "\n".join(details_lines),
                False,
            )

        post_passed, post_messages = run_post_checks(
            temp_dir,
            runtime_case.get("post_checks"),
        )
        details_lines.extend(["--- POST CHECKS ---", *post_messages])

        if not post_passed:
            failed_messages = [message for message in post_messages if "FAIL" in message]
            return (
                False,
                "; ".join(failed_messages) if failed_messages else "Post checks failed",
                "\n".join(details_lines),
                False,
            )

        return True, "Module main() check passed", "\n".join(details_lines), False

    except Exception as exc:  # pylint: disable=broad-exception-caught
        details_lines.append(traceback.format_exc())
        return False, f"Unhandled exception: {exc}", "\n".join(details_lines), True

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def validate_output_expectations(
    case_def: dict[str, Any],
    stdout: str,
    stderr: str,
) -> tuple[bool, list[str]]:
    conditions: list[str] = []
    passed = True

    expected_exit_code = case_def.get("expect_exit_code")
    expect_exit_nonzero = bool(case_def.get("expect_exit_nonzero", False))

    if expected_exit_code is not None and not isinstance(expected_exit_code, int):
        raise ConfigError("'expect_exit_code' must be an integer")

    if case_def.get("_actual_exit_code") is not None:
        actual_exit_code = case_def["_actual_exit_code"]
        if expected_exit_code is not None:
            if actual_exit_code != expected_exit_code:
                passed = False
                conditions.append(
                    f"Expected exit code {expected_exit_code}, got {actual_exit_code}"
                )
        elif expect_exit_nonzero and actual_exit_code == 0:
            passed = False
            conditions.append("Expected a non-zero exit code")

    expect_output = case_def.get("expect_output")
    if expect_output is not None:
        candidates = (
            [expect_output]
            if isinstance(expect_output, str)
            else ensure_list_of_strings(expect_output, "expect_output")
        )
        merged = stdout + "\n" + stderr
        for candidate in candidates:
            if candidate not in merged:
                passed = False
                conditions.append(f"output missing text: {candidate}")

    stdout_contains = case_def.get("expect_stdout_contains")
    if stdout_contains is not None:
        if not isinstance(stdout_contains, str):
            raise ConfigError("'expect_stdout_contains' must be a string")
        if stdout_contains not in stdout:
            passed = False
            conditions.append(f"stdout missing text: {stdout_contains}")

    stderr_contains = case_def.get("expect_stderr_contains")
    if stderr_contains is not None:
        if not isinstance(stderr_contains, str):
            raise ConfigError("'expect_stderr_contains' must be a string")
        if stderr_contains not in stderr:
            passed = False
            conditions.append(f"stderr missing text: {stderr_contains}")

    either_contains = case_def.get("expect_stdout_or_stderr_contains")
    if either_contains is not None:
        candidates = (
            [either_contains]
            if isinstance(either_contains, str)
            else ensure_list_of_strings(
                either_contains,
                "expect_stdout_or_stderr_contains",
            )
        )
        for candidate in candidates:
            if candidate not in stdout and candidate not in stderr:
                passed = False
                conditions.append(f"stdout/stderr missing text: {candidate}")

    expect_stdout_or_stderr_regex = case_def.get("expect_stdout_or_stderr_regex")
    if expect_stdout_or_stderr_regex is not None:
        candidates = (
            [expect_stdout_or_stderr_regex]
            if isinstance(expect_stdout_or_stderr_regex, str)
            else ensure_list_of_strings(
                expect_stdout_or_stderr_regex,
                "expect_stdout_or_stderr_regex",
            )
        )
        merged = stdout + "\n" + stderr
        for pattern in candidates:
            if re.search(pattern, merged, flags=re.MULTILINE) is None:
                passed = False
                conditions.append(f"stdout/stderr missing regex: {pattern}")

    expect_exit_code_in = case_def.get("expect_exit_code_in")
    if expect_exit_code_in is not None:
        if not isinstance(expect_exit_code_in, list) or not all(
            isinstance(item, int) for item in expect_exit_code_in
        ):
            raise ConfigError("'expect_exit_code_in' must be a list of integers")
        actual_exit_code = case_def.get("_actual_exit_code")
        if actual_exit_code not in expect_exit_code_in:
            passed = False
            conditions.append(
                f"Expected exit code in {expect_exit_code_in}, got {actual_exit_code}"
            )

    return passed, conditions


def check_path_exists(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    raw_path = case_def.get("path")
    if not isinstance(raw_path, str):
        raise ConfigError("'path_exists' requires a string 'path'")
    resolved = Path(expand_template(raw_path, work_dir, file_path))
    passed = resolved.exists()
    message = f"Path exists: {resolved}" if passed else f"Path not found: {resolved}"
    return passed, message, "", False


def check_path_not_exists(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    raw_path = case_def.get("path")
    if not isinstance(raw_path, str):
        raise ConfigError("'path_not_exists' requires a string 'path'")
    resolved = Path(expand_template(raw_path, work_dir, file_path))
    passed = not resolved.exists()
    message = (
        f"Path correctly absent: {resolved}"
        if passed
        else f"Path unexpectedly exists: {resolved}"
    )
    return passed, message, "", False


def run_command_assertion(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[CommandRunResult, list[str], bool]:
    spec = {
        "command": case_def.get("command"),
        "args": case_def.get("args", []),
        "shell": case_def.get("shell", False),
        "timeout_sec": case_def.get("timeout_sec", DEFAULT_CLI_TIMEOUT_SEC),
        "cwd": case_def.get("cwd"),
        "env": case_def.get("env"),
    }
    result = execute_command_spec(file_path, spec, work_dir)
    details = [
        f"Command: {result.command_text}",
        f"Shell mode: {'yes' if result.shell_mode else 'no'}",
        f"Timeout seconds: {result.timeout_sec}",
        f"Timed out: {'yes' if result.timed_out else 'no'}",
        f"Exit code: {result.exit_code}",
        "--- STDOUT ---",
        result.stdout.rstrip(),
        "--- STDERR ---",
        result.stderr.rstrip(),
    ]
    return result, details, result.timed_out


def check_command_success(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    result, details, timed_out = run_command_assertion(file_path, case_def, work_dir)
    if timed_out:
        return (
            False,
            f"Command timed out after {result.timeout_sec}s",
            "\n".join(details),
            True,
        )
    passed = result.exit_code == 0
    message = (
        "Command succeeded"
        if passed
        else f"Expected exit code 0, got {result.exit_code}"
    )
    return passed, message, "\n".join(details), False


def check_command_exit_code(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    expected_exit_code = case_def.get("expect_exit_code")
    if not isinstance(expected_exit_code, int):
        raise ConfigError("'command_exit_code' requires integer 'expect_exit_code'")
    result, details, timed_out = run_command_assertion(file_path, case_def, work_dir)
    if timed_out:
        return (
            False,
            f"Command timed out after {result.timeout_sec}s",
            "\n".join(details),
            True,
        )
    passed = result.exit_code == expected_exit_code
    message = (
        f"Command exited with expected code {expected_exit_code}"
        if passed
        else f"Expected exit code {expected_exit_code}, got {result.exit_code}"
    )
    return passed, message, "\n".join(details), False


def check_command_output_contains(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    expect_text = case_def.get("expect_text")
    if not isinstance(expect_text, str):
        raise ConfigError("'command_output_contains' requires string 'expect_text'")
    result, details, timed_out = run_command_assertion(file_path, case_def, work_dir)
    if timed_out:
        return (
            False,
            f"Command timed out after {result.timeout_sec}s",
            "\n".join(details),
            True,
        )
    merged = result.stdout + "\n" + result.stderr
    passed = expect_text in merged
    message = (
        f"Command output contains {expect_text!r}"
        if passed
        else f"Command output missing {expect_text!r}"
    )
    return passed, message, "\n".join(details), False


def check_command_output_regex(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    expect_pattern = case_def.get("expect_pattern")
    if not isinstance(expect_pattern, str):
        raise ConfigError("'command_output_regex' requires string 'expect_pattern'")
    result, details, timed_out = run_command_assertion(file_path, case_def, work_dir)
    if timed_out:
        return (
            False,
            f"Command timed out after {result.timeout_sec}s",
            "\n".join(details),
            True,
        )
    merged = result.stdout + "\n" + result.stderr
    passed = re.search(expect_pattern, merged, flags=re.MULTILINE) is not None
    message = (
        f"Command output matches regex {expect_pattern!r}"
        if passed
        else f"Command output does not match regex {expect_pattern!r}"
    )
    return passed, message, "\n".join(details), False


def build_cli_command(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[list[str] | str, bool]:
    raw_args = case_def.get("args", [])
    command_override = case_def.get("command")
    shell_mode = bool(case_def.get("shell", False))

    expanded_args = [expand_template(arg, work_dir, file_path) for arg in raw_args]

    if command_override:
        command_value = expand_template(command_override, work_dir, file_path)
        if shell_mode:
            parts = [command_value, *[shlex.quote(arg) for arg in expanded_args]]
            return " ".join(parts), True
        return [command_value, *expanded_args], False

    base_cmd = [sys.executable, str(file_path), *expanded_args]
    if shell_mode:
        return " ".join(shlex.quote(part) for part in base_cmd), True
    return base_cmd, False


def build_cli_env(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> dict[str, str]:
    env = os.environ.copy()
    raw_env = case_def.get("env")
    if raw_env is None:
        return env

    if not isinstance(raw_env, dict):
        raise ConfigError("'env' must be a mapping")

    for key, value in raw_env.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConfigError("'env' entries must be string -> string")
        env[key] = expand_template(value, work_dir, file_path)

    return env


def check_cli(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    prepare_case_files(work_dir, case_def)

    stdin_text = case_def.get("stdin")
    if stdin_text is not None and not isinstance(stdin_text, str):
        raise ConfigError("'stdin' must be a string")

    timeout_sec = case_def.get("timeout_sec", DEFAULT_CLI_TIMEOUT_SEC)
    if not isinstance(timeout_sec, int) or timeout_sec <= 0:
        raise ConfigError("'timeout_sec' must be a positive integer")

    expect_timeout = bool(case_def.get("expect_timeout", False))
    cmd, shell_mode = build_cli_command(file_path, case_def, work_dir)
    run_env = build_cli_env(file_path, case_def, work_dir)

    timed_out = False
    stdout = ""
    stderr = ""
    exit_code: int | None = None

    try:
        completed = subprocess.run(
            cmd,
            cwd=str(work_dir),
            capture_output=True,
            text=True,
            input=stdin_text,
            check=False,
            env=run_env,
            timeout=timeout_sec,
            shell=shell_mode,
        )
        stdout = normalize_completed_stream(completed.stdout)
        stderr = normalize_completed_stream(completed.stderr)
        exit_code = completed.returncode
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        stdout = normalize_completed_stream(exc.stdout)
        stderr = normalize_completed_stream(exc.stderr)

    case_def["_actual_exit_code"] = exit_code

    conditions: list[str] = []
    output_passed = True

    if timed_out:
        if expect_timeout:
            output_passed = True
        else:
            output_passed = False
            conditions.append(f"CLI command timed out after {timeout_sec}s")
    else:
        if expect_timeout:
            output_passed = False
            conditions.append("Expected command to time out, but it completed")
        else:
            output_passed, output_conditions = validate_output_expectations(
                case_def, stdout, stderr
            )
            conditions.extend(output_conditions)

    post_passed, post_messages = run_post_checks(work_dir, case_def.get("post_checks"))
    conditions.extend(message for message in post_messages if "FAIL" in message)

    passed = output_passed and post_passed and not conditions

    command_text = cmd if isinstance(cmd, str) else " ".join(cmd)
    details_lines = [
        f"Command: {command_text}",
        f"Shell mode: {'yes' if shell_mode else 'no'}",
        f"Timeout seconds: {timeout_sec}",
        f"Timed out: {'yes' if timed_out else 'no'}",
        f"Exit code: {exit_code}",
        "--- STDOUT ---",
        stdout.rstrip(),
        "--- STDERR ---",
        stderr.rstrip(),
    ]
    if post_messages:
        details_lines.extend(["--- POST CHECKS ---", *post_messages])

    message = "CLI check passed" if passed else "; ".join(conditions)
    message = sanitize_xml_text(message)
    details = sanitize_xml_text("\n".join(details_lines).strip())

    is_error = timed_out and not expect_timeout
    return passed, message, details, is_error


CHECK_HANDLERS: dict[
    str,
    Callable[[Path, dict[str, Any], Path], tuple[bool, str, str, bool]],
] = {
    "file_exists": check_file_exists,
    "py_compile": check_py_compile,
    "source_contains": check_source_contains,
    "source_contains_any": check_source_contains_any,
    "source_contains_all": check_source_contains_all,
    "function_exists": check_function_exists,
    "function_exists_any": check_function_exists_any,
    "main_guard": check_main_guard,
    "cli": check_cli,
    "py_function": check_py_function,
    "module_main_with_env": check_module_main_with_env,
    "path_exists": check_path_exists,
    "path_not_exists": check_path_not_exists,
    "command_success": check_command_success,
    "command_exit_code": check_command_exit_code,
    "command_output_contains": check_command_output_contains,
    "command_output_regex": check_command_output_regex,
}


def run_single_check(
    file_path: Path,
    case_def: dict[str, Any],
    work_dir: Path,
) -> tuple[bool, str, str, bool]:
    case_type = case_def.get("type", "cli")
    if not isinstance(case_type, str):
        raise ConfigError("Case 'type' must be a string")

    apply_skip_controls(file_path, case_def, work_dir)

    handler = CHECK_HANDLERS.get(case_type)
    if handler is None:
        raise ConfigError(f"Unsupported test type: {case_type}")

    return handler(file_path, case_def, work_dir)


def get_file_work_dir(suite_name: str, file_entry: str) -> Path:
    return (
        REPORTS_DIR
        / "_work"
        / sanitize_name(suite_name)
        / sanitize_name(Path(file_entry).stem)
    )


def append_combined_case_log(
    file_work_dir: Path,
    testcase_name: str,
    status: str,
    message: str,
    details: str,
) -> None:
    file_work_dir.mkdir(parents=True, exist_ok=True)
    log_path = file_work_dir / "combined.log"

    lines = [
        "=" * 80,
        f"TEST CASE: {testcase_name}",
        f"STATUS: {status}",
        f"MESSAGE: {message}",
    ]

    if details:
        lines.extend(["", details.rstrip()])

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines).rstrip())
        handle.write("\n\n")


def run_case(
    suite_name: str,
    file_entry: str,
    case_index: int,
    case_def: dict[str, Any],
    options: RunCaseOptions | None = None,
) -> TestOutcome:
    if options is None:
        options = RunCaseOptions()

    file_path = resolve_target_path(file_entry)
    case_name = case_def["name"].strip()
    testcase_name = f"{suite_name}::{Path(file_entry).name}::{case_name}"

    if options.allure_enabled:
        allure.dynamic.title(testcase_name)
        allure.dynamic.description(f"Test case from {suite_name} for {file_entry}")
        allure.dynamic.feature(suite_name)
        allure.dynamic.story(case_name)
        allure.dynamic.severity(allure.severity_level.NORMAL)
    case_type = str(case_def.get("type", "cli"))

    file_work_dir = get_file_work_dir(suite_name, file_entry)
    work_dir = file_work_dir / sanitize_name(f"{case_index}_{case_name}")
    work_dir.mkdir(parents=True, exist_ok=True)

    effective_case = dict(case_def)
    if options.suite_command is not None and "command" not in effective_case:
        effective_case["command"] = options.suite_command

    meta = TestMeta(
        suite_name=suite_name,
        phase="case",
        test_type=case_type,
    )

    try:
        passed, message, details, is_error = run_single_check(
            file_path, effective_case, work_dir
        )
        outcome = create_outcome(
            testcase_name=testcase_name,
            file_path=file_entry,
            passed=passed,
            message=sanitize_xml_text(message),
            meta=meta,
            details=sanitize_xml_text(details),
            error=is_error,
        )
        if options.allure_enabled:
            allure.attach(
                outcome.details,
                name="Test Details",
                attachment_type=allure.attachment_type.TEXT,
            )
            if not passed:
                allure.attach(
                    message,
                    name="Failure Message",
                    attachment_type=allure.attachment_type.TEXT,
                )
        status = "ERROR" if is_error else ("PASS" if passed else "FAIL")
        append_combined_case_log(
            file_work_dir=file_work_dir,
            testcase_name=testcase_name,
            status=status,
            message=outcome.message,
            details=outcome.details,
        )
        return outcome
    except SkipCase as exc:
        outcome = create_outcome(
            testcase_name=testcase_name,
            file_path=file_entry,
            passed=True,
            message=sanitize_xml_text(str(exc)),
            meta=meta,
            details=sanitize_xml_text(
                f"Case skipped in work directory: {work_dir}\nReason: {exc}"
            ),
            skipped=True,
        )
        if options.allure_enabled:
            allure.attach(
                outcome.details,
                name="Skip Details",
                attachment_type=allure.attachment_type.TEXT,
            )
        append_combined_case_log(
            file_work_dir=file_work_dir,
            testcase_name=testcase_name,
            status="SKIPPED",
            message=outcome.message,
            details=outcome.details,
        )
        return outcome
    except ConfigError as exc:
        outcome = create_outcome(
            testcase_name=testcase_name,
            file_path=file_entry,
            passed=False,
            message=sanitize_xml_text(
                format_outcome_message("Configuration error", str(exc))
            ),
            meta=meta,
            details=sanitize_xml_text(traceback.format_exc()),
            error=True,
        )
        if options.allure_enabled:
            allure.attach(
                outcome.details,
                name="Error Details",
                attachment_type=allure.attachment_type.TEXT,
            )
        append_combined_case_log(
            file_work_dir=file_work_dir,
            testcase_name=testcase_name,
            status="ERROR",
            message=outcome.message,
            details=outcome.details,
        )
        return outcome
    except Exception as exc:  # pylint: disable=broad-exception-caught
        outcome = create_outcome(
            testcase_name=testcase_name,
            file_path=file_entry,
            passed=False,
            message=sanitize_xml_text(
                format_outcome_message("Unhandled exception", str(exc))
            ),
            meta=meta,
            details=sanitize_xml_text(traceback.format_exc()),
            error=True,
        )
        if options.allure_enabled:
            allure.attach(
                outcome.details,
                name="Exception Details",
                attachment_type=allure.attachment_type.TEXT,
            )
        append_combined_case_log(
            file_work_dir=file_work_dir,
            testcase_name=testcase_name,
            status="ERROR",
            message=outcome.message,
            details=outcome.details,
        )
        return outcome


def write_junit_xml(
    xml_report: Path,
    suite_name: str,
    yaml_file: Path,
    outcomes: list[TestOutcome],
) -> None:
    tests = len(outcomes)
    failures = sum(
        1 for item in outcomes if not item.passed and not item.error and not item.skipped
    )
    errors = sum(1 for item in outcomes if item.error)
    skipped = sum(1 for item in outcomes if item.skipped)
    passed = sum(1 for item in outcomes if item.passed and not item.skipped)

    testsuite = xml_et.Element("testsuite")
    testsuite.set("name", sanitize_xml_text(suite_name))
    testsuite.set("tests", str(tests))
    testsuite.set("failures", str(failures))
    testsuite.set("errors", str(errors))
    testsuite.set("skipped", str(skipped))

    properties = xml_et.SubElement(testsuite, "properties")

    yaml_prop = xml_et.SubElement(properties, "property")
    yaml_prop.set("name", "yaml_file")
    yaml_prop.set(
        "value",
        sanitize_xml_text(yaml_file.relative_to(PROJECT_ROOT).as_posix()),
    )

    passed_prop = xml_et.SubElement(properties, "property")
    passed_prop.set("name", "passed")
    passed_prop.set("value", sanitize_xml_text(str(passed)))

    for outcome in outcomes:
        testcase = xml_et.SubElement(testsuite, "testcase")
        testcase.set(
            "classname",
            sanitize_xml_text(sanitize_name(outcome.file_path or suite_name)),
        )
        testcase.set("name", sanitize_xml_text(outcome.testcase_name))
        testcase.set("file", sanitize_xml_text(outcome.file_path))

        if outcome.skipped:
            skipped_node = xml_et.SubElement(testcase, "skipped")
            skipped_node.set("message", sanitize_xml_text(outcome.message))
            skipped_node.text = sanitize_xml_text(outcome.details)
        elif not outcome.passed and outcome.error:
            error_node = xml_et.SubElement(testcase, "error")
            error_node.set("message", sanitize_xml_text(outcome.message))
            error_node.text = sanitize_xml_text(outcome.details)
        elif not outcome.passed:
            failure_node = xml_et.SubElement(testcase, "failure")
            failure_node.set("message", sanitize_xml_text(outcome.message))
            failure_node.text = sanitize_xml_text(outcome.details)

        system_out = xml_et.SubElement(testcase, "system-out")
        body = [
            f"file={outcome.file_path}",
            f"suite={outcome.meta.suite_name}",
            f"phase={outcome.meta.phase}",
            f"type={outcome.meta.test_type}",
            f"message={outcome.message}",
        ]
        if outcome.details:
            body.extend(["details:", outcome.details])

        system_out.text = sanitize_xml_text("\n".join(body))

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    xml_et.ElementTree(testsuite).write(
        xml_report,
        encoding="utf-8",
        xml_declaration=True,
    )


def print_group_summary(
    suite_name: str,
    outcomes: list[TestOutcome],
    xml_report: Path,
) -> None:
    total = len(outcomes)
    passed = sum(1 for item in outcomes if item.passed and not item.skipped)
    failures = sum(
        1 for item in outcomes if not item.passed and not item.error and not item.skipped
    )
    errors = sum(1 for item in outcomes if item.error)
    warnings = sum(1 for item in outcomes if item.skipped)

    print(f"\n{color_text('[INFO]', Color.BLUE)} Finished group : {suite_name}")
    print(
        f"{color_text('[INFO]', Color.BLUE)} XML report     : "
        f"{xml_report.relative_to(PROJECT_ROOT).as_posix()}"
    )
    print(f"{color_text('Total   :', Color.BLUE)} {total}")
    print(f"{color_text('Passed  :', Color.GREEN)} {passed}")
    print(f"{color_text('Failed  :', Color.RED)} {failures}")
    print(f"{color_text('Errors  :', Color.RED)} {errors}")
    print(f"{color_text('Warnings:', Color.YELLOW)} {warnings}")

    failed_items = [item for item in outcomes if not item.passed and not item.skipped]
    if failed_items:
        print(color_text("\n[FAILURES]", Color.RED))
        for item in failed_items:
            kind = "ERROR" if item.error else "FAIL"
            print(
                color_text(f"  - [{kind}] ", Color.RED)
                + f"{item.file_path} :: {item.testcase_name} :: {item.message}"
            )

    warning_items = [item for item in outcomes if item.skipped]
    if warning_items:
        print(color_text("\n[WARNINGS]", Color.YELLOW))
        for item in warning_items:
            print(
                color_text("  - [WARNING] ", Color.YELLOW)
                + f"{item.file_path} :: {item.testcase_name} :: {item.message}"
            )


def build_config_error_outcome(
    yaml_file: Path,
    message: str,
    details: str,
) -> TestOutcome:
    return create_outcome(
        testcase_name="config::load_yaml",
        file_path=yaml_file.relative_to(PROJECT_ROOT).as_posix(),
        passed=False,
        message=sanitize_xml_text(message),
        meta=TestMeta(
            suite_name="config",
            phase="config",
            test_type="config",
        ),
        details=sanitize_xml_text(details),
        error=True,
    )


def git_changed_paths_for_query(args: list[str]) -> set[Path]:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return set()

    paths: set[Path] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        paths.add((PROJECT_ROOT / line).resolve())
    return paths


def get_recently_changed_paths() -> set[Path]:
    changed: set[Path] = set()
    changed.update(git_changed_paths_for_query(["diff", "--name-only"]))
    changed.update(git_changed_paths_for_query(["diff", "--cached", "--name-only"]))
    changed.update(
        git_changed_paths_for_query(["ls-files", "--others", "--exclude-standard"])
    )
    return changed


def normalize_target_for_matching(file_entry: str) -> Path:
    return resolve_target_path(file_entry)


def get_yaml_target_entries(yaml_file: Path) -> set[str]:
    try:
        config = load_yaml_config(yaml_file)
        suites = normalize_suites(config)
    except ConfigError:
        return set()

    targets: set[str] = set()
    for suite in suites:
        for file_entry in suite["files"]:
            targets.add(Path(file_entry).as_posix())
    return targets


def yaml_targets_changed_files(yaml_file: Path, changed_paths: set[Path]) -> set[str]:
    try:
        config = load_yaml_config(yaml_file)
        suites = normalize_suites(config)
    except ConfigError:
        return set()

    matched: set[str] = set()
    for suite in suites:
        for file_entry in suite["files"]:
            target_path = normalize_target_for_matching(file_entry)
            if target_path in changed_paths:
                matched.add(Path(file_entry).as_posix())
    return matched


def select_yaml_runs(yaml_files: list[Path]) -> list[tuple[Path, set[str]]]:
    changed_paths = get_recently_changed_paths()

    if not changed_paths:
        return []

    selected_map: dict[Path, set[str]] = {}

    for yaml_file in yaml_files:
        yaml_abs = yaml_file.resolve()
        matched_targets = yaml_targets_changed_files(yaml_file, changed_paths)

        if yaml_abs in changed_paths:
            matched_targets.update(get_yaml_target_entries(yaml_file))

        if matched_targets:
            selected_map[yaml_file] = matched_targets

    return list(selected_map.items())


def select_yaml_runs_for_target(
    yaml_files: list[Path],
    target: str,
) -> list[tuple[Path, set[str]]]:
    target_path = resolve_target_path(target)
    selected_runs: list[tuple[Path, set[str]]] = []

    for yaml_file in yaml_files:
        try:
            config = load_yaml_config(yaml_file)
            suites = normalize_suites(config)
        except ConfigError:
            continue

        matched_targets: set[str] = set()
        for suite in suites:
            for file_entry in suite["files"]:
                if resolve_target_path(file_entry) == target_path:
                    matched_targets.add(Path(file_entry).as_posix())

        if matched_targets:
            selected_runs.append((yaml_file, matched_targets))

    return selected_runs


def run_yaml(
    yaml_file: Path,
    selected_targets: set[str],
    allure_enabled: bool = False,
) -> int:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    group_name = get_group_name(yaml_file)
    xml_report = build_report_path(group_name, yaml_file)

    print(f"\n{color_text('[INFO]', Color.BLUE)} Running group : {group_name}")
    print(
        f"{color_text('[INFO]', Color.BLUE)} YAML file     : "
        f"{yaml_file.relative_to(PROJECT_ROOT).as_posix()}"
    )
    print(
        f"{color_text('[INFO]', Color.BLUE)} XML report    : "
        f"{xml_report.relative_to(PROJECT_ROOT).as_posix()}"
    )

    try:
        config = load_yaml_config(yaml_file)
        suites = normalize_suites(config)
    except ConfigError as exc:
        outcome = build_config_error_outcome(
            yaml_file=yaml_file,
            message=format_outcome_message("Configuration error", str(exc)),
            details=traceback.format_exc(),
        )
        write_junit_xml(xml_report, group_name, yaml_file, [outcome])
        print_group_summary(group_name, [outcome], xml_report)
        return 1

    outcomes: list[TestOutcome] = []

    for suite_index, suite in enumerate(suites, start=1):
        suite_name = suite["name"]
        run_case_options = RunCaseOptions(
            suite_command=suite.get("command"),
            allure_enabled=allure_enabled,
        )
        suite_files = suite["files"]
        suite_cases = suite["cases"]

        filtered_files = []
        for file_entry in suite_files:
            normalized_file_entry = Path(file_entry).as_posix()
            if normalized_file_entry in selected_targets:
                filtered_files.append(file_entry)

        if not filtered_files:
            continue

        print(f"{color_text('[INFO]', Color.BLUE)} Suite         : {suite_name}")

        if not suite_cases:
            outcomes.append(
                create_outcome(
                    testcase_name=f"{suite_name}::no_cases",
                    file_path="",
                    passed=False,
                    message="Suite has no cases defined",
                    meta=TestMeta(
                        suite_name=suite_name,
                        phase="suite",
                        test_type="config",
                    ),
                    details=f"suites[{suite_index}] has an empty 'cases' list.",
                    error=True,
                )
            )
            continue

        for file_entry in filtered_files:
            print(f"{color_text('[INFO]', Color.BLUE)} Target file   : {file_entry}")
            for case_index, case_def in enumerate(suite_cases, start=1):
                outcomes.append(
                    run_case(
                        suite_name=suite_name,
                        file_entry=file_entry,
                        case_index=case_index,
                        case_def=case_def,
                        options=run_case_options,
                    )
                )

    if not outcomes:
        return 0

    write_junit_xml(xml_report, group_name, yaml_file, outcomes)
    print_group_summary(group_name, outcomes, xml_report)
    return 0 if all(item.passed or item.skipped for item in outcomes) else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run YAML-driven tests for impacted files or a manual target."
    )
    parser.add_argument(
        "--target",
        help="Run tests only for the specified Python file target from YAML suites",
    )
    parser.add_argument(
        "--enable-allure",
        action="store_true",
        help="Enable Allure reporting for enhanced test visualization",
    )
    parser.add_argument(
        "--allure-report-dir",
        default="reports/allure-report",
        help="Directory to store Allure HTML report (default: reports/allure-report)",
    )
    args = parser.parse_args()

    allure_enabled = False

    if args.enable_allure:
        if not ALLURE_AVAILABLE:
            print(
                f"{color_text('WARNING:', Color.YELLOW)} "
                "Allure not available. Install allure-pytest for enhanced reporting. "
                "Continuing without Allure."
            )
        else:
            allure_report_dir = Path(args.allure_report_dir)
            allure_report_dir.mkdir(parents=True, exist_ok=True)
            os.environ["ALLURE_RESULTS_DIR"] = str(allure_report_dir / "allure-results")
            allure_enabled = True

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_old_pytest_xml_reports()

    yaml_files = discover_yaml_files()

    if not yaml_files:
        reason = (
            f"No YAML files found in "
            f"{TEST_YAML_DIR.relative_to(PROJECT_ROOT).as_posix()}"
        )
        print(f"{color_text('WARNING:', Color.YELLOW)} {reason}")
        create_placeholder_xml(reason)
        return 0

    remove_placeholder_xml()

    if args.target:
        print(
            f"{color_text('[INFO]', Color.BLUE)} "
            f"Manual target override enabled: {args.target}"
        )
        selected_runs = select_yaml_runs_for_target(yaml_files, args.target)
    else:
        selected_runs = select_yaml_runs(yaml_files)

    if not selected_runs:
        if args.target:
            message = f"No YAML test groups found for manual target: {args.target}"
        else:
            message = "No impacted YAML test groups found for changed files."

        print(f"{color_text('[INFO]', Color.BLUE)} {message}")
        print(
            f"{color_text('[INFO]', Color.BLUE)} Reports written to: "
            f"{REPORTS_DIR.relative_to(PROJECT_ROOT).as_posix()}"
        )
        create_placeholder_xml(message)
        return 0

    if args.target:
        print(color_text("[INFO] Running YAML test groups for manual target:", Color.BLUE))
    else:
        print(color_text("[INFO] Running only impacted YAML test groups:", Color.BLUE))

    for yaml_file, selected_targets in selected_runs:
        print(f"  - {yaml_file.relative_to(PROJECT_ROOT).as_posix()}")
        for target in sorted(selected_targets):
            print(f"      * target: {target}")

    overall_exit_code = 0
    for yaml_file, selected_targets in selected_runs:
        exit_code = run_yaml(
            yaml_file,
            selected_targets=selected_targets,
            allure_enabled=allure_enabled,
        )
        if exit_code != 0:
            overall_exit_code = exit_code

    if allure_enabled:
        print(f"\n{color_text('[INFO]', Color.BLUE)} Generating Allure report...")
        allure_results_dir = Path(os.environ["ALLURE_RESULTS_DIR"])
        allure_report_dir = allure_results_dir.parent
        try:
            subprocess.run(
                [
                    "allure",
                    "generate",
                    str(allure_results_dir),
                    "-o",
                    str(allure_report_dir),
                    "--clean",
                ],
                check=True,
            )
            print(
                f"{color_text('[INFO]', Color.BLUE)} "
                f"Allure report generated at: {allure_report_dir}"
            )
        except subprocess.CalledProcessError as exc:
            print(
                f"{color_text('WARNING:', Color.YELLOW)} "
                f"Failed to generate Allure report: {exc}"
            )

    print(f"\n{color_text('[INFO]', Color.BLUE)} Custom YAML test execution completed.")
    print(
        f"{color_text('[INFO]', Color.BLUE)} Reports written to: "
        f"{REPORTS_DIR.relative_to(PROJECT_ROOT).as_posix()}"
    )
    return overall_exit_code


if __name__ == "__main__":
    sys.exit(main())
