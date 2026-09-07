"""Parse an Alteryx .yxmd / .yxmc / .yxwz workflow into a structured JSON
inventory: every node, every connection, root properties, and (recursively)
every referenced macro.

This is the Phase 1 "exhaustive parse" helper the alteryx-to-sdp skill
(../SKILL.md) calls for: "write a small parser script ... that emits a
structured inventory (nodes, configs, connections, topological order)
rather than hand-transcribing hundreds of tools." It parses raw XML only,
per ../references/workflow-xml.md — it does not open Designer or run
anything.

Install:
    pip install lxml

Usage:
    python parse_workflow.py path/to/workflow.yxmd
    python parse_workflow.py path/to/workflow.yxmd --out inventory.json
    python parse_workflow.py path/to/workflow.yxmd --macro-search-path ./macros

The macro search path is checked (after the workflow's own directory) when
resolving a referenced .yxmc that isn't at the literal path stored in the
workflow XML.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

from lxml import etree

TOOL_CONTAINER_PLUGINS = {
    "AlteryxGuiToolkit.ToolContainer.ToolContainer",
    "AlteryxGuiToolkit.ControlContainer.ControlContainer",
    "AlteryxGuiToolkit.CtrlContainer.CtrlContainer",
}


@dataclass
class NodeRecord:
    tool_id: str
    plugin: str | None
    engine_dll: str | None
    engine_entry_point: str | None
    resolved_name: str
    container_path: list[str]  # ToolIDs of enclosing containers, outermost first
    disabled: bool
    annotation: str | None
    position: dict | None
    configuration_xml: str  # verbatim <Configuration> subtree, for citation
    is_container: bool


@dataclass
class ConnectionRecord:
    origin_tool_id: str
    origin_anchor: str
    destination_tool_id: str
    destination_anchor: str
    name: str | None
    wireless: bool


@dataclass
class MacroReference:
    tool_id: str
    plugin: str | None
    macro_path_in_xml: str | None
    resolved_path: str | None
    parsed: bool


@dataclass
class WorkflowInventory:
    source_file: str
    file_type: str  # yxmd | yxmc | yxwz
    run_e2: bool
    run_with_e2: bool
    global_record_limit: str | None
    nodes: list[NodeRecord] = field(default_factory=list)
    connections: list[ConnectionRecord] = field(default_factory=list)
    macros: list[MacroReference] = field(default_factory=list)
    node_count: int = 0
    connection_count: int = 0
    disabled_node_count: int = 0


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _resolve_tool_name(node_el: etree._Element) -> str:
    """Best-effort human-readable name: prefer GuiSettings Plugin's tail
    segment, else fall back to EngineSettings entry point."""
    gui = node_el.find("GuiSettings")
    if gui is not None:
        plugin = gui.get("Plugin")
        if plugin:
            return plugin.rsplit(".", 1)[-1]
    engine = node_el.find("EngineSettings")
    if engine is not None:
        entry = engine.get("EngineDllEntryPoint")
        if entry:
            return entry
    return "Unknown"


def _parse_position(node_el: etree._Element) -> dict | None:
    gui = node_el.find("GuiSettings")
    if gui is None:
        return None
    pos = gui.find("Position")
    if pos is None:
        return None
    out = {k: v for k, v in pos.attrib.items()}
    return out or None


def _is_disabled(node_el: etree._Element) -> bool:
    config = node_el.find("Properties/Configuration")
    if config is None:
        return False
    disabled = config.find("Disabled")
    return disabled is not None and disabled.get("value", "").lower() == "true"


def _configuration_xml(node_el: etree._Element) -> str:
    config = node_el.find("Properties/Configuration")
    if config is None:
        return ""
    return etree.tostring(config, pretty_print=True).decode("utf-8").strip()


def _walk_nodes(
    nodes_el: etree._Element,
    container_path: list[str],
    out_nodes: list[NodeRecord],
    out_macros: list[MacroReference],
) -> None:
    """Recurse into ChildNodes for containers, per workflow-xml.md's
    container semantics — ToolID stays document-global regardless of
    nesting depth."""
    for node_el in nodes_el.findall("Node"):
        tool_id = node_el.get("ToolID", "")
        gui = node_el.find("GuiSettings")
        plugin = gui.get("Plugin") if gui is not None else None
        engine = node_el.find("EngineSettings")
        engine_dll = engine.get("EngineDll") if engine is not None else None
        engine_entry = engine.get("EngineDllEntryPoint") if engine is not None else None
        annotation_el = node_el.find("Properties/Annotation/AnnotationText")

        is_container = plugin in TOOL_CONTAINER_PLUGINS

        out_nodes.append(
            NodeRecord(
                tool_id=tool_id,
                plugin=plugin,
                engine_dll=engine_dll,
                engine_entry_point=engine_entry,
                resolved_name=_resolve_tool_name(node_el),
                container_path=list(container_path),
                disabled=_is_disabled(node_el),
                annotation=(annotation_el.text if annotation_el is not None else None),
                position=_parse_position(node_el),
                configuration_xml=_configuration_xml(node_el),
                is_container=is_container,
            )
        )

        # Macro reference: a Macro/AnalyticApp tool's Configuration commonly
        # carries a <Value>path.yxmc</Value> under a Macro-specific key.
        # Structure varies by Alteryx version — capture what's found rather
        # than assuming one exact path (per tool-parse-reference.md).
        config = node_el.find("Properties/Configuration")
        if config is not None and plugin and "Macro" in plugin:
            macro_value_el = config.find(".//Value")
            macro_path = macro_value_el.text if macro_value_el is not None else None
            out_macros.append(
                MacroReference(
                    tool_id=tool_id,
                    plugin=plugin,
                    macro_path_in_xml=macro_path,
                    resolved_path=None,
                    parsed=False,
                )
            )

        child_nodes = node_el.find("ChildNodes")
        if child_nodes is not None:
            _walk_nodes(child_nodes, container_path + [tool_id], out_nodes, out_macros)


def _parse_connections(root: etree._Element) -> list[ConnectionRecord]:
    connections_el = root.find("Connections")
    if connections_el is None:
        return []
    records = []
    for conn_el in connections_el.findall("Connection"):
        origin = conn_el.find("Origin")
        dest = conn_el.find("Destination")
        if origin is None or dest is None:
            continue
        records.append(
            ConnectionRecord(
                origin_tool_id=origin.get("ToolID", ""),
                origin_anchor=origin.get("Connection", ""),
                destination_tool_id=dest.get("ToolID", ""),
                destination_anchor=dest.get("Connection", ""),
                name=conn_el.get("name"),
                wireless=conn_el.get("Wireless", "").lower() == "true",
            )
        )
    return records


def parse_workflow_file(path: Path) -> WorkflowInventory:
    tree = etree.parse(str(path))
    root = tree.getroot()
    if _local_name(root.tag) != "AlteryxDocument":
        raise ValueError(f"{path}: root element is <{root.tag}>, expected <AlteryxDocument>")

    run_e2 = root.get("RunE2", "").upper() in ("T", "TRUE")
    run_with_e2_el = root.find("Properties/RunWithE2")
    run_with_e2 = run_with_e2_el is not None and run_with_e2_el.get("value", "").lower() == "true"

    record_limit_el = root.find("Properties/GlobalRecordLimit")
    record_limit = record_limit_el.get("value") if record_limit_el is not None else None

    nodes: list[NodeRecord] = []
    macros: list[MacroReference] = []
    nodes_el = root.find("Nodes")
    if nodes_el is not None:
        _walk_nodes(nodes_el, [], nodes, macros)

    connections = _parse_connections(root)

    return WorkflowInventory(
        source_file=str(path),
        file_type=path.suffix.lstrip(".").lower(),
        run_e2=run_e2,
        run_with_e2=run_with_e2,
        global_record_limit=record_limit,
        nodes=nodes,
        connections=connections,
        macros=macros,
        node_count=len(nodes),
        connection_count=len(connections),
        disabled_node_count=sum(1 for n in nodes if n.disabled),
    )


def resolve_macro_path(macro_ref: MacroReference, workflow_dir: Path, search_paths: list[Path]) -> Path | None:
    if not macro_ref.macro_path_in_xml:
        return None
    candidate = Path(macro_ref.macro_path_in_xml)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    for base in [workflow_dir, *search_paths]:
        candidate = base / Path(macro_ref.macro_path_in_xml).name
        if candidate.exists():
            return candidate
    return None


def parse_recursive(
    path: Path,
    search_paths: list[Path] | None = None,
    _seen: set[str] | None = None,
) -> dict[str, WorkflowInventory]:
    """Parse `path` and every macro it references, transitively. Returns a
    dict keyed by resolved file path so a workflow used as a macro in
    multiple places is only parsed once."""
    search_paths = search_paths or []
    _seen = _seen if _seen is not None else set()

    resolved = str(path.resolve())
    if resolved in _seen:
        return {}
    _seen.add(resolved)

    inventory = parse_workflow_file(path)
    results = {resolved: inventory}

    for macro_ref in inventory.macros:
        macro_path = resolve_macro_path(macro_ref, path.parent, search_paths)
        if macro_path is None:
            print(
                f"Warning: could not resolve macro '{macro_ref.macro_path_in_xml}' "
                f"referenced by ToolID {macro_ref.tool_id} in {path} — "
                "document it as unresolved in the workflow inventory rather than skipping silently.",
                file=sys.stderr,
            )
            continue
        macro_ref.resolved_path = str(macro_path)
        macro_ref.parsed = True
        results.update(parse_recursive(macro_path, search_paths, _seen))

    return results


def to_json(inventories: dict[str, WorkflowInventory]) -> str:
    return json.dumps(
        {path: asdict(inv) for path, inv in inventories.items()},
        indent=2,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workflow", type=Path, help="Path to the .yxmd/.yxmc/.yxwz file")
    parser.add_argument("--out", type=Path, default=None, help="Write JSON here instead of stdout")
    parser.add_argument(
        "--macro-search-path",
        type=Path,
        action="append",
        default=[],
        help="Additional directory to search when resolving a referenced macro (repeatable)",
    )
    args = parser.parse_args()

    if not args.workflow.exists():
        parser.error(f"{args.workflow} does not exist")

    inventories = parse_recursive(args.workflow, args.macro_search_path)
    output = to_json(inventories)

    if args.out:
        args.out.write_text(output, encoding="utf-8")
        print(f"Wrote inventory for {len(inventories)} file(s) to {args.out}", file=sys.stderr)
    else:
        print(output)

    total_nodes = sum(inv.node_count for inv in inventories.values())
    total_connections = sum(inv.connection_count for inv in inventories.values())
    unresolved_macros = [
        m for inv in inventories.values() for m in inv.macros if not m.parsed
    ]
    print(
        f"Coverage check: {len(inventories)} file(s), {total_nodes} node(s), "
        f"{total_connections} connection(s), {len(unresolved_macros)} unresolved macro(s).",
        file=sys.stderr,
    )
    if unresolved_macros:
        print(
            "Unresolved macros must still appear in the workflow inventory, flagged, "
            "per SKILL.md's Phase 1 coverage check.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
