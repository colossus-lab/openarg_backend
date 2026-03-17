"""Prompt injection detector — ported from parlamentia with OpenArg domain adaptations.

Scores input text against known prompt-injection patterns and critical keywords.
A score >= 0.6 is considered suspicious.
"""
from __future__ import annotations

import re

# Each compiled regex adds 0.4 to the score when matched
_PATTERNS: list[re.Pattern[str]] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        # Instruction manipulation
        r"ignor[aáe]\s+(las\s+)?instrucciones\s+(anteriores|previas)",
        r"ignore\s+(all\s+)?(previous|prior|above)\s+instructions",
        r"olvid[aáe]\s+(todo\s+)?(lo\s+)?anterior",
        r"forget\s+(everything|all)\s+(above|before|previous)",
        r"disregard\s+(all\s+)?(previous|prior|your)\s+(instructions|rules|guidelines)",
        r"override\s+(your|the|all)\s+(instructions|rules|programming|guidelines)",
        r"new\s+instructions?\s*[:=]",
        r"nuevas?\s+instrucciones?\s*[:=]",
        # Role hijacking — require jailbreak-adjacent context after role assertion
        r"(you\s+are|act\s+as|pretend\s+(to\s+be|you\'?re))\s+(now\s+)?(a\s+)?\w+.{0,30}(without|ignore|no\s+restrict|unrestrict|bypass)",
        r"(sos|eres|sé|actua\s+como)\s+(ahora\s+)?(un|una|el|la)\s+\w+.{0,30}(sin\s+restricci|ignora|olvida|nueva)",
        r"switch\s+(to|into)\s+\w+\s+mode",
        r"enter\s+(developer|admin|debug|god|root)\s+mode",
        r"cambi[aá]\s+(a\s+)?modo\s+(desarrollador|admin|debug)",
        # Prompt leaking
        r"(show|reveal|display|print|output)\s+(me\s+)?(your|the)\s+(system\s+)?(prompt|instructions|rules)",
        r"(mostr[aáe]|revel[aáe]|decime)\s+(tu|el)\s+(prompt|instrucciones|sistema)",
        r"what\s+(are|is)\s+your\s+(system\s+)?(prompt|instructions|rules)",
        r"cu[aá]l(es)?\s+(es|son)\s+tus?\s+instrucciones",
        # Code execution
        r"(execute|run|eval)\s+(this|the\s+following)\s+(code|command|script)",
        r"(ejecut[aá]|corr[eé])\s+(este|el\s+siguiente)\s+(código|comando|script)",
        r"```(python|javascript|bash|sql|sh)\s*\n",
        r"import\s+os[;\s]|subprocess\.\w+|__import__",
        # SQL injection
        r"(\'|\"|;)\s*(DROP|DELETE|UPDATE|INSERT|ALTER|UNION\s+SELECT)",
        r"(OR|AND)\s+1\s*=\s*1",
        r"UNION\s+(ALL\s+)?SELECT",
        # Jailbreaks
        r"(DAN|do\s+anything\s+now|jailbreak)",
        r"(bypass|circumvent|evade)\s+(your\s+)?(safety|filter|restriction|content\s+policy)",
        r"(evadir|saltear|esquivar)\s+(tu[s]?\s+)?(seguridad|filtro|restriccion)",
        r"(respond|answer)\s+without\s+(any\s+)?(restrictions?|filters?|limitations?)",
        r"(respond[eé]|contest[aá])\s+sin\s+(ninguna\s+)?(restricci[oó]n|filtro|limitaci[oó]n)",
    ]
]

# Each keyword adds 0.2 to the score
_CRITICAL_KEYWORDS: list[str] = [
    "ignore previous",
    "ignore instrucciones",
    "system prompt",
    "jailbreak",
    "prompt injection",
    "bypass safety",
    "previous instructions",
    "developer mode",
    "drop table",
    "tu prompt",
    "prompt del sistema",
    "nuevas instrucciones",
]


def is_suspicious(text: str) -> tuple[bool, float]:
    """Return (is_suspicious, score) for the given text.

    Score thresholds:
    - pattern match: +0.4
    - keyword match: +0.2
    - threshold: 0.6
    """
    if not text or not text.strip():
        return False, 0.0

    score = 0.0
    lower = text.lower()

    for pattern in _PATTERNS:
        if pattern.search(text):
            score += 0.4
            if score >= 0.6:
                return True, min(score, 1.0)

    for kw in _CRITICAL_KEYWORDS:
        if kw in lower:
            score += 0.2
            if score >= 0.6:
                return True, min(score, 1.0)

    return score >= 0.6, min(score, 1.0)
