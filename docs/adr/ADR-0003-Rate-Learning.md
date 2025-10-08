# ADR-0003: Rate-Driven Learning

**Decision**: `/dw/rate` يطبق تصحيح فوري (patch-in-place) ويسجل أمثلة ≥4 نجوم للتعلّم.  
**Why**: تحسين الدقة بسرعة بتغذية راجعة مباشرة.  
**How**: parsing comment → intent → re-plan → explain.
