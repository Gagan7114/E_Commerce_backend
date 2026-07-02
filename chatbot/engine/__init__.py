"""The chatbot engine: turns a natural-language question into a data-backed
answer (and optionally an Excel export).

Public entry point:

    from chatbot.engine import answer_question
    result = answer_question(user, conversation, "excel of blinkit alerts")

`result` is an EngineResult (see engine.py).
"""

from .engine import EngineResult, answer_question, engine_mode  # noqa: F401
