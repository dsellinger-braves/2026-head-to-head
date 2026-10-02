# Python Execution & Scripting Rules

This document establishes operational constraints for writing and executing Python commands within this workspace.

---

## 1. Avoid Inline `python3 -c '...'` for Complex or Interpolated Code

### The Fatal Trap
In Python < 3.12, **backslashes are strictly forbidden inside f-string expressions**:
```python
# ❌ SyntaxError: unexpected character after line continuation character
print(f"Slot {k[\"keeper_slot\"]}")
```
When running commands via `python3 -c '...'`, single quotes wrap the entire shell command. Attempting to escape inner double quotes inside an f-string expression (`{\"...\"]}`) immediately crashes Python's parser.

### Required Pattern A: Quoted Heredoc (`python3 - << 'EOF'`)
When running multi-line Python scripts via bash, **always use a single-quoted heredoc**:
```bash
python3 - << 'EOF'
import json, requests

# Single quotes and double quotes can be used naturally without escaping collisions:
for k in records:
    print(f"  Slot {k['keeper_slot']}: {k['player_name']}")
EOF
```
*Note: Quoting `'EOF'` instructs bash to pass all content completely raw without variable expansion or quote stripping.*

### Required Pattern B: Standalone Script via `write_to_file`
For complex data inspections, write a script to the conversation artifact scratch directory or execute an existing utility in `scripts/`:
```bash
python3 scripts/inspect_db.py keepers --season 2027 --team 5
```

---

## 2. Best Practices for Dictionary Indexing in F-Strings

Even outside bash one-liners, always assign dictionary lookup values to local variables prior to string interpolation:

```python
# ✅ Clean, readable, and 100% portable across all Python versions:
slot = k.get("keeper_slot")
name = k.get("player_name")
cost = k.get("cost")
print(f"  Slot {slot}: {name} (${cost})")
```

---

## 3. Python Module Path Constraints

- Do **not** attempt to import from `scratch` (e.g. `import scratch.utils`).
- The workspace root is the only guaranteed directory on `sys.path`.
- When helper functions are needed, place them in `pipelines/`, `scripts/`, or import standard library modules directly.
