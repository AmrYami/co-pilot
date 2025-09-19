import os
import threading
from typing import Any, Dict, Optional

_MODELS: Dict[str, Optional[Dict[str, Any]]] = {}
_LOCK = threading.Lock()


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------
# SQLCoder (ExLlamaV2) loader
# ---------------------------

def _load_sql_model() -> Optional[Dict[str, Any]]:
    backend = os.getenv("MODEL_BACKEND", "exllama")
    path = os.getenv("MODEL_PATH")
    if not path:
        raise RuntimeError("MODEL_PATH not set for SQL model")
    if backend != "exllama":
        raise RuntimeError(f"Unsupported MODEL_BACKEND={backend} for SQL model")

    from core.sqlcoder_exllama import load_exllama_generator

    cfg = {
        "max_seq_len": _env_int("MODEL_MAX_SEQ_LEN", 4096),
        "max_new_tokens": _env_int("GENERATION_MAX_NEW_TOKENS", 256),
        "temperature": float(os.getenv("GENERATION_TEMPERATURE", "0.2")),
        "top_p": float(os.getenv("GENERATION_TOP_P", "0.9")),
        "stop": [tok for tok in os.getenv("STOP", "</s>,<|im_end|>").split(",") if tok],
    }

    handle = load_exllama_generator(model_path=path, config=cfg)
    _log("SQL model (SQLCoder/ExLlamaV2) ready")
    return {
        "role": "sql",
        "backend": backend,
        "path": path,
        "handle": handle,
        "gen_cfg": cfg,
    }


# --------------------------------------
# Clarifier (HuggingFace 4-bit/FP16) loader
# --------------------------------------

def _resolve_device_for_inputs(model: Any, device_map: str | None) -> Any:
    if device_map and device_map not in {"auto", "balanced"}:
        import torch

        try:
            return torch.device(device_map)
        except Exception:
            pass
    if hasattr(model, "device"):
        return model.device
    try:
        import torch

        return next(model.parameters()).device
    except Exception:
        return None


def _load_clarifier_model() -> Optional[Dict[str, Any]]:
    backend = os.getenv("CLARIFIER_MODEL_BACKEND", "off").lower()
    if backend in {"off", "none", "disabled"}:
        _log("[clarifier] disabled by config")
        return None

    path = os.getenv("CLARIFIER_MODEL_PATH")
    if not path:
        raise RuntimeError("CLARIFIER_MODEL_PATH not set")

    device_map = os.getenv("CLARIFIER_DEVICE_MAP", "cuda:0")
    quant_type = os.getenv("CLARIFIER_QUANT_TYPE", "nf4")
    low_cpu_mem = _env_bool("CLARIFIER_LOW_CPU_MEM", True)
    dtype = os.getenv("CLARIFIER_COMPUTE_DTYPE", "float16")

    if backend not in {"hf-4bit", "hf-fp16"}:
        raise RuntimeError(f"Unsupported CLARIFIER_MODEL_BACKEND={backend}")

    _log(f"[clarifier] loading HF model: {path} ({backend}) on {device_map}")

    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    try:
        from transformers import BitsAndBytesConfig
    except ImportError:
        BitsAndBytesConfig = None  # type: ignore[assignment]

    load_kwargs: Dict[str, Any] = {"trust_remote_code": True}
    if backend == "hf-4bit":
        if BitsAndBytesConfig is None:
            raise RuntimeError("bitsandbytes is required for hf-4bit clarifier backend")
        load_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type=quant_type,
            bnb_4bit_compute_dtype=getattr(torch, dtype, torch.float16),
        )
    else:
        load_kwargs["torch_dtype"] = getattr(torch, dtype, torch.float16)

    if device_map:
        load_kwargs["device_map"] = device_map
    if low_cpu_mem:
        load_kwargs["low_cpu_mem_usage"] = True

    tokenizer = AutoTokenizer.from_pretrained(path, use_fast=True, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(path, trust_remote_code=True, **load_kwargs)
    model.eval()

    default_cfg = {
        "max_new_tokens": _env_int("CLARIFIER_MAX_NEW_TOKENS", 128),
        "temperature": float(os.getenv("CLARIFIER_TEMPERATURE", "0.0")),
        "top_p": float(os.getenv("CLARIFIER_TOP_P", "0.9")),
        "stop": [tok for tok in os.getenv("CLARIFIER_STOP", "").split(",") if tok],
    }

    target_device = _resolve_device_for_inputs(model, device_map)

    class _HFGenerator:
        def __init__(self, mdl, tok, defaults, device):
            self.model = mdl
            self.tokenizer = tok
            self.defaults = defaults
            self.device = device

        def generate(
            self,
            prompt: str,
            max_new_tokens: Optional[int] = None,
            temperature: Optional[float] = None,
            top_p: Optional[float] = None,
            stop: Optional[list[str]] = None,
        ) -> str:
            cfg = dict(self.defaults)
            if max_new_tokens is not None:
                cfg["max_new_tokens"] = int(max_new_tokens)
            if temperature is not None:
                cfg["temperature"] = float(temperature)
            if top_p is not None:
                cfg["top_p"] = float(top_p)
            stops = stop if stop is not None else cfg.get("stop")

            inputs = self.tokenizer(prompt, return_tensors="pt")
            if self.device is not None:
                inputs = {k: v.to(self.device) for k, v in inputs.items()}

            with torch.inference_mode():
                outputs = self.model.generate(
                    **inputs,
                    max_new_tokens=cfg["max_new_tokens"],
                    do_sample=cfg["temperature"] > 0,
                    temperature=cfg["temperature"],
                    top_p=cfg["top_p"],
                    pad_token_id=self.tokenizer.eos_token_id,
                )
            text = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            for token in stops or []:
                idx = text.find(token)
                if idx >= 0:
                    text = text[:idx]
            return text.strip()

    handle = _HFGenerator(model, tokenizer, default_cfg, target_device)
    _log("[clarifier] model ready")
    return {
        "role": "clarifier",
        "backend": backend,
        "path": path,
        "handle": handle,
        "tokenizer": tokenizer,
        "model": model,
        "device_map": device_map,
    }


def load_llm(role: str) -> Optional[Dict[str, Any]]:
    """Load a model for the given role ("sql" or "clarifier")."""

    with _LOCK:
        if role in _MODELS and _MODELS[role] is not None:
            _log(f"Reusing cached model for role={role}")
            return _MODELS[role]

        if role == "sql":
            _MODELS[role] = _load_sql_model()
        elif role == "clarifier":
            _MODELS[role] = _load_clarifier_model()
        else:
            raise ValueError(f"Unknown model role: {role}")
        return _MODELS[role]


def model_info() -> Dict[str, Any]:
    sql = _MODELS.get("sql")
    clar = _MODELS.get("clarifier")

    if sql is None:
        try:
            sql = load_llm("sql")
        except Exception:
            sql = None

    if clar is None:
        try:
            clar = load_llm("clarifier")
        except Exception:
            clar = None

    return {
        "mode": "dw-pipeline",
        "llm": {
            "backend": (sql or {}).get("backend") if sql else "unknown",
            "path": (sql or {}).get("path") if sql else None,
        },
        "clarifier": {
            "backend": (clar or {}).get("backend") if clar else "disabled",
            "path": (clar or {}).get("path") if clar else None,
            "device_map": (clar or {}).get("device_map") if clar else None,
        },
    }


# ------------------------------------------------------------------
# Backwards-compatible helpers
# ------------------------------------------------------------------

def load_model(settings: Any | None = None) -> Optional[Any]:
    payload = load_llm("sql")
    return payload.get("handle") if payload else None


def load_clarifier(settings: Any | None = None) -> Optional[Any]:
    payload = load_llm("clarifier")
    return payload.get("handle") if payload else None


def load_llm_from_settings(settings: Any | None = None):
    payload = load_llm("sql")
    if not payload:
        return None, {}
    info = {
        "backend": payload.get("backend"),
        "path": payload.get("path"),
        "name": os.getenv("MODEL_NAME") or os.path.basename(payload.get("path", "")) or "sqlcoder",
    }
    return payload.get("handle"), info
