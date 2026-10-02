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

---

## 4. Zero Speculative / Blind File Access

### The Anti-Pattern
Agents often speculate or guess file paths inside Python one-liners, resulting in fatal `FileNotFoundError`:
```bash
# ❌ NEVER GUESS PATHS:
python3 -c "import json; d=json.load(open('src/data/historicalDrafts.json')); ..."
# -> FileNotFoundError: [Errno 2] No such file or directory: 'src/data/historicalDrafts.json'
```

### Operational Rules:
1. **Discover Before Executing**:
   - Always run `list_dir` on the target directory or use `grep_search` to find actual filenames before executing a script that references a file path.
   - For inspecting files or JSON structures, use the native `view_file` tool rather than running `python3 -c "open('...')"` in bash.
2. **Always Guard File Opens**:
   - Whenever writing Python code that accesses a local file path, explicitly verify file existence or catch `FileNotFoundError`:
```python
from pathlib import Path
import json

file_path = Path('src/data/draft2026.json')
if not file_path.is_file():
    print(f"⚠️ Target file not found: {file_path}")
else:
    data = json.loads(file_path.read_text())
    print(f"Loaded {len(data)} items")
```

---

## 5. Canonical Data Files in `src/data/`

To prevent guessing file names, refer to the verified datasets in `src/data/`:
- `draft2026.json`: Completed 2026 draft records (288 picks).
- `draftAssetTrades2026.json`: Offseason traded draft picks.
- `compensationPicks2026.json`: Compensation picks awarded.
- `keeperInput2026.json` / `keeperInput2027.json`: Keeper selections per owner.
- `teamBudgets2026.json` / `teamBudgets2027.json`: FAAB/keeper budgets and draft penalties.
- `keeperCalculations.json`: Z-score multi-year blended player valuations, PRs, and ranks.
- `historicalFinishes.json`: Multi-year final standings and roto point totals (2012–2025).
- `historicalTrades.json`: Historical trades and keeper asset movements.
- `transactions2026.json`: In-season roster moves (adds, drops, trades).

