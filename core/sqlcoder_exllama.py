import os
import logging
from dataclasses import dataclass
from types import SimpleNamespace

# ---- ExLlamaV2 imports (version-agnostic) -----------------------------------
from exllamav2.config import ExLlamaV2Config
from exllamav2.model import ExLlamaV2
from exllamav2.cache import ExLlamaV2Cache
from exllamav2.generator.base import ExLlamaV2BaseGenerator

# Tokenizer class name/location changed across versions
try:
    from exllamav2.tokenizer import ExLlamaV2Tokenizer as Tokenizer
except Exception:
    from exllamav2.tokenizer import Tokenizer

# Sampler settings moved into generator.sampler
try:
    from exllamav2.generator.sampler import ExLlamaV2Sampler
except Exception:
    # Some very old builds put it in a different place
    try:
        from exllamav2.generator import ExLlamaV2Sampler  # type: ignore
    except Exception:
        ExLlamaV2Sampler = None  # type: ignore

log = logging.getLogger("core.sqlcoder_exllama")

# -----------------------------------------------------------------------------
# Bundle returned from loader
# -----------------------------------------------------------------------------
@dataclass
class ExllamaBundle:
    model: ExLlamaV2
    tokenizer: Tokenizer
    cache: ExLlamaV2Cache
    generator: ExLlamaV2BaseGenerator
    config: ExLlamaV2Config


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default




def _build_sampler_settings(temp: float, top_p: float):
    try:
        settings_cls = getattr(ExLlamaV2Sampler, 'Settings')
    except Exception:
        settings_cls = None

    settings = None
    if settings_cls is not None:
        try:
            settings = settings_cls()
        except Exception:
            settings = None

    if settings is None:
        settings = SimpleNamespace()

    defaults = {
        'temperature': float(temp),
        'top_p': float(top_p),
        'top_k': 0,
        'min_p': 0.0,
        'typical': None,
        'tfs': None,
        'repetition_penalty': 1.0,
        'penalty_range': 64,
        'presence_penalty': 0.0,
        'frequency_penalty': 0.0,
        'mirostat': None,
        'cfg_scale': 1.0,
    }
    for key, value in defaults.items():
        if not hasattr(settings, key):
            setattr(settings, key, value)
        elif key in {'temperature', 'top_p'}:
            setattr(settings, key, value)
    return settings

def load_exllama_generator(model_dir: str) -> ExllamaBundle:
    """
    Create an ExLlamaV2 stack (config, model, tokenizer, cache, generator).
    Robust to minor API differences across exllamav2 versions.
    """
    log.info(f"[exl2] Loading ExLlamaV2 model from: {model_dir}")

    # Config
    try:
        cfg = ExLlamaV2Config(model_dir)
    except TypeError:
        # older form may require set attribute / prepare call
        cfg = ExLlamaV2Config()
        cfg.model_dir = model_dir
    try:
        cfg.prepare()
    except Exception:
        # Not all versions require/allow prepare()
        pass

    # Model
    model = ExLlamaV2(cfg)

    # Tokenizer
    try:
        tokenizer = Tokenizer(cfg)
    except Exception:
        # Fallback if constructor expects dir
        tokenizer = Tokenizer(model_dir)

    # Cache
    max_seq_len = _env_int("EXL2_CACHE_MAX_SEQ_LEN", 2048)
    try:
        cache = ExLlamaV2Cache(model, max_seq_len, lazy=True)
    except TypeError:
        # older signature
        cache = ExLlamaV2Cache(model, max_seq_len)

    # Generator **MUST** be constructed (model, cache, tokenizer)
    generator = ExLlamaV2BaseGenerator(model, cache, tokenizer)

    log.info(f"[exl2] Ready: cache.max_seq_len={getattr(cache, 'max_seq_len', 'n/a')}")
    return ExllamaBundle(model=model, tokenizer=tokenizer, cache=cache, generator=generator, config=cfg)


class SQLCoderExllama:
    """
    Thin wrapper around ExLlamaV2BaseGenerator exposing a stable .generate()
    suitable for the DW pipeline.
    """

    def __init__(self, bundle: ExllamaBundle):
        self.generator = bundle.generator
        self.tokenizer = bundle.tokenizer
        self.cache = bundle.cache

        # Build sampler settings once
        temp = _env_float("GENERATION_TEMPERATURE", 0.2)
        nucleus = _env_float("GENERATION_TOP_P", 0.9)
        self.settings = _build_sampler_settings(temp, nucleus)

        # You can add more knobs here if needed:
        # self.settings.repetition_penalty = _env_float("GENERATION_REP_PENALTY", 1.0)

    def _encode_tokens(self, text: str, *, add_bos: bool = True):
        try:
            return self.tokenizer.encode(text, add_bos=add_bos, encode_special_tokens=False)
        except TypeError:
            tokens = self.tokenizer.encode(text)
            if not add_bos:
                bos_id = getattr(self.tokenizer, 'bos_token_id', None)
                if bos_id is not None:
                    if hasattr(tokens, 'shape') and tokens.shape[-1] > 0:
                        if tokens.ndim == 2 and tokens.shape[0] > 0 and tokens[0, 0].item() == bos_id:
                            tokens = tokens[:, 1:]
                        elif tokens.ndim == 1 and tokens[0].item() == bos_id:
                            tokens = tokens[1:]
                    elif isinstance(tokens, (list, tuple)) and tokens and tokens[0] == bos_id:
                        tokens = tokens[1:]
            return tokens

    def _token_length(self, tokens) -> int:
        if hasattr(tokens, 'shape'):
            return int(tokens.shape[-1])
        return len(tokens)

    def _decode_tokens(self, tokens):
        try:
            return self.tokenizer.decode(tokens)
        except TypeError:
            return self.tokenizer.decode(tokens)

    def _tail_tokens(self, tokens, limit: int):
        if limit <= 0:
            return tokens
        if hasattr(tokens, 'shape'):
            return tokens[:, -limit:]
        return tokens[-limit:]

    def _apply_stops(self, text: str, stop):
        if not stop:
            return text
        best = len(text)
        for s in stop:
            idx = text.find(s)
            if idx != -1 and idx < best:
                best = idx
        return text[:best]

    def generate(self, prompt: str, max_new_tokens: int = 256, stop=None) -> str:
        """
        Generate text with ExLlamaV2, respecting cache limits and handling
        differences in generate_simple signature across versions.
        """
        # Compute safe new tokens given cache size and prompt length
        reserve = _env_int("EXL2_INPUT_RESERVE_TOKENS", 64)
        max_ctx = getattr(self.cache, "max_seq_len", 2048)
        max_prompt_tokens = max(1, max_ctx - reserve)

        tokens = self._encode_tokens(prompt)
        in_tokens = self._token_length(tokens)
        if in_tokens > max_prompt_tokens:
            tokens = self._tail_tokens(tokens, max_prompt_tokens)
            prompt = self._decode_tokens(tokens)
            in_tokens = self._token_length(tokens)
            log.warning(
                "[exl2.gen] prompt trimmed to last %s tokens to respect cache",
                in_tokens,
            )

        available = max_ctx - in_tokens - reserve
        target_new = max_new_tokens if max_new_tokens is not None else 256
        allow = max(0, min(int(target_new), max(0, available)))

        log.info(
            "[exl2.gen] tokens_in=%s allow_new=%s max_req=%s max_ctx=%s reserve=%s",
            in_tokens, allow, int(target_new), max_ctx, reserve
        )

        if allow == 0:
            log.warning("[exl2.gen] no room for new tokens; returning empty string")
            return ""

        # Try 3-arg signature (prompt, settings, num_tokens) first
        try:
            text = self.generator.generate_simple(prompt, self.settings, allow)
        except TypeError:
            # Fall back to 2-arg (prompt, num_tokens)
            text = self.generator.generate_simple(prompt, allow)

        # Post-process string stops (ExLlamaV2 base does not universally accept stop strings)
        text = self._apply_stops(text, stop)

        try:
            out_tokens = self._token_length(self._encode_tokens(text, add_bos=False))
        except Exception:
            out_tokens = None
        log.info(
            "[exl2.gen] output_chars=%s output_tokens=%s",
            len(text),
            out_tokens,
        )
        return text


# Convenience loader used by core/model_loader.py
def build_sql_model(model_dir: str) -> SQLCoderExllama:
    bundle = load_exllama_generator(model_dir)
    return SQLCoderExllama(bundle)
