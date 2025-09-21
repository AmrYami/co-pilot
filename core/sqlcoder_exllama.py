import os, re, logging
from dataclasses import dataclass

log = logging.getLogger("core.sqlcoder_exllama")

# ----- ExLlamaV2 0.3.x imports -----
try:
    from exllamav2 import ExLlamaV2, ExLlamaV2Config, ExLlamaV2Cache
    from exllamav2.tokenizer import ExLlamaV2Tokenizer
    from exllamav2.generator import (
        ExLlamaV2BaseGenerator,
        ExLlamaV2SamplingSettings,
    )
except Exception as e:
    raise ImportError(
        "ExLlamaV2 >= 0.3.2 is required. Please `pip install exllamav2==0.3.2`."
    ) from e


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_i(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _extract_sql_fence(text: str) -> str:
    """
    Return the content inside the first ```sql ... ``` or ``` ... ``` block.
    If none found, try to salvage a SELECT/WITH line.
    """
    if not text:
        return ""

    # ```sql ... ```
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.S | re.I)
    if m:
        return m.group(1).strip()

    # ``` ... ```
    m = re.search(r"```\s*(.*?)```", text, flags=re.S)
    if m:
        return m.group(1).strip()

    # salvage the first SELECT/WITH onwards
    m = re.search(r"(?is)\b(SELECT|WITH)\b.*", text)
    if m:
        return m.group(0).strip()

    return ""


@dataclass
class _Bundle:
    model: ExLlamaV2
    tok: ExLlamaV2Tokenizer
    cache: ExLlamaV2Cache
    gen: ExLlamaV2BaseGenerator
    max_seq_len: int


def _load_bundle(model_path: str) -> _Bundle:
    log.info(f"[exl2] Loading ExLlamaV2 model: {model_path}")

    cfg = ExLlamaV2Config(model_path)
    max_seq_len = _env_i("EXL2_CACHE_MAX_SEQ_LEN", 2048)
    cfg.set_max_seq_len(max_seq_len)  # 0.3.x API

    # Optional lower memory behavior
    if os.getenv("EXL2_FORCE_BASE", "0") == "1":
        cfg.set_option("force_base", True)

    model = ExLlamaV2(cfg)
    tok = ExLlamaV2Tokenizer(cfg)

    cache = ExLlamaV2Cache(model, max_seq_len=max_seq_len, batch_size=1)
    gen = ExLlamaV2BaseGenerator(model, tok, cache)  # <-- correct arg order

    # Optional: autosplit by GB strings, if available in this build
    split = os.getenv("EXL2_GPU_SPLIT_GB", "").strip()
    reserve_gb = _env_f("RESERVE_VRAM_GB", 0.0)
    if hasattr(model, "load_autosplit"):
        try:
            # exllamav2 will read VRAM sizes; if not provided, it will try its own heuristic
            gb_list = [float(x) for x in split.split(",")] if split else None
            log.info(f"[exl2] Calling load_autosplit (split={gb_list}, reserve={reserve_gb} GB)")
            model.load_autosplit(gpu_split=gb_list, reserve_vram=reserve_gb)
        except TypeError:
            # Fallback older signature
            log.info("[exl2] load_autosplit signature mismatch; calling without kwargs")
            try:
                if split:
                    model.load_autosplit([float(x) for x in split.split(",")])
                else:
                    model.load_autosplit()
            except Exception as e:
                log.warning(f"[exl2] load_autosplit failed, falling back to model.load(): {e}")
                model.load()
    else:
        model.load()

    log.info("[exl2] ExLlamaV2 model ready")
    return _Bundle(model=model, tok=tok, cache=cache, gen=gen, max_seq_len=max_seq_len)


class SQLCoderExLlama:
    def __init__(self, bundle: _Bundle):
        self.bundle = bundle
        self.gen = bundle.gen
        self.tok = bundle.tok
        self.max_seq_len = bundle.max_seq_len

        # Static sampling defaults (can be adjusted via env)
        self.temperature = _env_f("GENERATION_TEMPERATURE", 0.2)
        self.top_p = _env_f("GENERATION_TOP_P", 0.9)

    def _settings(self) -> ExLlamaV2SamplingSettings:
        s = ExLlamaV2SamplingSettings()
        s.temperature = self.temperature
        s.top_p = self.top_p
        # We keep the rest defaults; stop strings are not handled by exllamav2 0.3.x in settings
        return s

    def _fit_new_tokens(self, prompt: str, requested_new: int) -> int:
        # Keep generation within cache limits
        ids = self.tok.encode(prompt)
        reserve = _env_i("EXL2_INPUT_RESERVE_TOKENS", 64)
        available = max(self.max_seq_len - len(ids) - reserve, 16)
        return max(1, min(requested_new, available))

    def generate(self, prompt: str, max_new_tokens: int = 192) -> str:
        settings = self._settings()
        n_new = self._fit_new_tokens(prompt, int(max_new_tokens))

        if os.getenv("LLM_TRACE", "0") == "1":
            log.info(f"[exl2] gen_start: new={n_new}, T={settings.temperature}, top_p={settings.top_p}")
        text = self.gen.generate_simple(prompt, settings, n_new)
        if os.getenv("LLM_TRACE", "0") == "1":
            log.info(f"[exl2] gen_result: size={len(text)}")
        return text

    def generate_sql(self, prompt: str, max_new_tokens: int = 192) -> dict:
        """
        Helper: run once, then try to extract fenced SQL.
        """
        raw = self.generate(prompt, max_new_tokens=max_new_tokens)
        sql = _extract_sql_fence(raw)
        return {"raw": raw, "sql": sql}


# public builder called by core/model_loader.py
def build_sql_model(model_path: str):
    bundle = _load_bundle(model_path)
    mdl = SQLCoderExLlama(bundle)
    log.info("[exl2] SQL model wrapper ready")
    return mdl
