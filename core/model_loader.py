import os
import threading
from typing import Any, Dict, Iterable, Optional

import torch

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

    stop_tokens = [tok for tok in os.getenv("STOP", "</s>,<|im_end|>").split(",") if tok]
    stop_tokens = [tok for tok in stop_tokens if "```" not in tok]

    cfg = {
        "max_seq_len": _env_int("MODEL_MAX_SEQ_LEN", 4096),
        "max_new_tokens": _env_int("GENERATION_MAX_NEW_TOKENS", 256),
        "temperature": float(os.getenv("GENERATION_TEMPERATURE", "0.2")),
        "top_p": float(os.getenv("GENERATION_TOP_P", "0.9")),
        "stop": stop_tokens,
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

    device_map_env = os.getenv("CLARIFIER_DEVICE_MAP", "auto").strip()
    device_map = device_map_env if device_map_env else "auto"
    low_cpu_mem = _env_bool("CLARIFIER_LOW_CPU_MEM", True)
    compute_dtype_str = os.getenv("CLARIFIER_COMPUTE_DTYPE", "float16")
    compute_dtype = getattr(torch, compute_dtype_str, torch.float16)
    quant_type = os.getenv("CLARIFIER_QUANT_TYPE", "nf4").lower()

    _log(
        f"[clarifier] loading HF model: {path} ({backend}) on {device_map}"
    )

    from transformers import AutoModelForCausalLM, AutoTokenizer

    load_kwargs: Dict[str, Any] = {
        "device_map": device_map,
        "low_cpu_mem_usage": low_cpu_mem,
        "torch_dtype": compute_dtype,
    }

    if backend == "hf-4bit":
        try:
            from transformers import BitsAndBytesConfig
        except ImportError as exc:  # pragma: no cover - import guard
            raise RuntimeError(
                "bitsandbytes is required for hf-4bit clarifier backend"
            ) from exc

        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=False,
            bnb_4bit_quant_type=quant_type,
            bnb_4bit_compute_dtype=compute_dtype,
        )
        load_kwargs["quantization_config"] = bnb_config
    elif backend in {"hf-fp16", "hf-fp8", "hf-fp32"}:
        pass
    else:
        raise RuntimeError(f"Unsupported CLARIFIER_MODEL_BACKEND={backend}")

    load_kwargs.pop("trust_remote_code", None)

    model = AutoModelForCausalLM.from_pretrained(
        path,
        trust_remote_code=True,
        **load_kwargs,
    )
    tokenizer = AutoTokenizer.from_pretrained(
        path,
        trust_remote_code=True,
        use_fast=True,
    )
    model.eval()

    clarifier_stop = [tok for tok in os.getenv("CLARIFIER_STOP", "").split(",") if tok]
    clarifier_stop = [tok for tok in clarifier_stop if "```" not in tok]

    default_cfg = {
        "max_new_tokens": _env_int("CLARIFIER_MAX_NEW_TOKENS", 128),
        "temperature": float(os.getenv("CLARIFIER_TEMPERATURE", "0.0")),
        "top_p": float(os.getenv("CLARIFIER_TOP_P", "0.9")),
        "stop": clarifier_stop,
    }

    target_device = _resolve_device_for_inputs(model, device_map)

    class _HFGenerator:
        def __init__(self, mdl, tok, defaults, device):
            self.model = mdl
            self.tokenizer = tok
            self.defaults = defaults
            self.device = device

        def _prepare_prompt(
            self,
            prompt: Optional[str] = None,
            *,
            system_prompt: Optional[str] = None,
            user_prompt: Optional[str] = None,
        ) -> str:
            if prompt:
                return prompt
            parts = []
            if system_prompt:
                parts.append(system_prompt.strip())
            if user_prompt:
                parts.append(user_prompt.strip())
            if not parts:
                raise ValueError("prompt or system/user prompts required")
            return "\n\n".join(parts).strip()

        def generate(
            self,
            prompt: Optional[str] = None,
            *,
            system_prompt: Optional[str] = None,
            user_prompt: Optional[str] = None,
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

            full_prompt = self._prepare_prompt(
                prompt, system_prompt=system_prompt, user_prompt=user_prompt
            )

            inputs = self.tokenizer(full_prompt, return_tensors="pt")
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
        "name": os.path.basename(path.rstrip("/")) or path,
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


def ensure_model(role: str) -> Optional[Any]:
    """Ensure a model for the role is loaded and return the handle."""

    payload = load_llm(role)
    if not payload:
        return None
    return payload.get("handle")


def get_model(role: str) -> Optional[Any]:
    """Return the cached model handle for the given role if available."""

    with _LOCK:
        payload = _MODELS.get(role)
    if payload is None:
        return ensure_model(role)
    if isinstance(payload, dict):
        return payload.get("handle")
    return None


def llm_complete(
    *,
    role: str,
    prompt: str,
    max_new_tokens: Optional[int] = None,
    temperature: Optional[float] = None,
    top_p: Optional[float] = None,
    stop: Optional[Iterable[str]] = None,
) -> str:
    """Generate text using the requested LLM role with sane defaults."""

    payload = load_llm(role)
    if not payload:
        return ""

    handle = payload.get("handle")
    if handle is None:
        return ""

    cfg = payload.get("gen_cfg") or {}

    kwargs: Dict[str, Any] = {}

    max_tokens = cfg.get("max_new_tokens") if max_new_tokens is None else max_new_tokens
    if max_tokens is not None:
        kwargs["max_new_tokens"] = int(max_tokens)

    temp = cfg.get("temperature") if temperature is None else temperature
    if temp is not None:
        kwargs["temperature"] = float(temp)

    nucleus = cfg.get("top_p") if top_p is None else top_p
    if nucleus is not None:
        kwargs["top_p"] = float(nucleus)

    stop_tokens = stop if stop is not None else cfg.get("stop")
    if stop_tokens:
        kwargs["stop"] = list(stop_tokens)

    try:
        text = handle.generate(prompt, **kwargs)
    except TypeError:
        kwargs.pop("stop", None)
        text = handle.generate(prompt, **kwargs)
    except Exception:
        return ""

    return text.strip() if isinstance(text, str) else ""


def model_info() -> Dict[str, Any]:
    def _ensure_payload(role: str) -> Optional[Dict[str, Any]]:
        payload = _MODELS.get(role)
        if payload is None:
            try:
                payload = load_llm(role)
            except Exception:
                payload = None
        return payload if isinstance(payload, dict) else None

    sql = _ensure_payload("sql")
    clar = _ensure_payload("clarifier")

    def _describe(role: str, payload: Optional[Dict[str, Any]]) -> str:
        if not payload:
            return "disabled" if role == "clarifier" else "unavailable"
        env_key = "MODEL_NAME" if role == "sql" else "CLARIFIER_MODEL_NAME"
        name = os.getenv(env_key)
        if not name:
            path = payload.get("path") or ""
            name = os.path.basename(path.rstrip("/")) or path or "unknown"
        backend = payload.get("backend")
        suffix = ""
        if role == "clarifier":
            device = payload.get("device_map")
            if backend and device:
                suffix = f" ({backend}, {device})"
            elif backend:
                suffix = f" ({backend})"
            elif device:
                suffix = f" ({device})"
        elif backend:
            suffix = f" ({backend})"
        return f"{name}{suffix}" if suffix else name

    return {
        "mode": "dw-pipeline",
        "llm": _describe("sql", sql),
        "clarifier": _describe("clarifier", clar),
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
