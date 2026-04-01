import sys
from pathlib import Path

# Ensure `import src.*` works when running pytest from any cwd.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

