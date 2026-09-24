"""分析链单测：构造边界（除权日、ST、次新、上市首日、制度切换、数据不足）。"""

import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parents[2]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))
