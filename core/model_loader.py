# core/model_loader.py
from __future__ import annotations
import os

def load_model(settings):
    backend = (settings.get("MODEL_BACKEND", "exllama") or "exllama").lower()
    model_path = settings.get("MODEL_PATH")
    if not model_path:
        raise ValueError("MODEL_PATH is required")

    # optional comma list, e.g. "hf-4bit,hf-8bit,hf-fp16"
    fallbacks = [(settings.get("MODEL_BACKEND_FALLBACKS") or "").strip().lower()]
    fallbacks = [b.strip() for b in fallbacks[0].split(",") if b.strip()] if fallbacks[0] else []

    tried = []

    def _try(backend_name: str):
        nonlocal tried
        tried.append(backend_name)
        if backend_name == "exllama":
            return _load_exllama(model_path, settings)
        elif backend_name == "hf-fp16":
            return _load_hf(model_path, torch_dtype="fp16")
        elif backend_name == "hf-8bit":
            return _load_hf(model_path, load_in_8bit=True)
        elif backend_name == "hf-4bit":
            return _load_hf(model_path, load_in_4bit=True)
        else:
            raise ValueError(f"Unknown MODEL_BACKEND: {backend_name}")

    # try primary, then fallbacks
    errors = []
    for b in [backend] + [fb for fb in fallbacks if fb != backend]:
        try:
            return _try(b)
        except Exception as e:
            errors.append(f"{b}: {e}")

    raise RuntimeError("All model backends failed: " + " | ".join(errors))


def _load_exllama(model_path, settings):
    # EXL2 4-bit (ExLlamaV2)
    from exllamav2 import ExLlamaV2Config, ExLlamaV2, ExLlamaV2Tokenizer, ExLlamaV2Cache
    from exllamav2.generator import ExLlamaV2Generator
    max_seq_len = int(settings.get("MODEL_MAX_SEQ_LEN", 4096) or 4096)
    cfg = ExLlamaV2Config(); cfg.model_dir = model_path
    model = ExLlamaV2(cfg)
    tok = ExLlamaV2Tokenizer(cfg)
    cache = ExLlamaV2Cache(model, batch_size=1, max_seq_len=max_seq_len)
    gen = ExLlamaV2Generator(model, tok, cache)

    def _gen(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9):
        return gen.generate_simple(prompt, max_new_tokens=max_new_tokens,
                                   temperature=temperature, top_p=top_p)
    return {"backend":"exllama","tokenizer":tok,"model":model,"generate":_gen}


def _load_hf(model_path, torch_dtype: str | None = None,
             load_in_8bit: bool = False, load_in_4bit: bool = False):
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig

    tok = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)

    bnb_cfg = None
    if load_in_8bit:
        bnb_cfg = BitsAndBytesConfig(load_in_8bit=True)
    if load_in_4bit:
        # 4-bit nf4 is widely used; you can tune this if needed
        bnb_cfg = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_use_double_quant=True,
                                     bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16)

    dtype_map = {"fp16": torch.float16, "bf16": torch.bfloat16, None: None}
    dtype = dtype_map.get(torch_dtype, None)

    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        trust_remote_code=True,
        device_map="auto",
        torch_dtype=dtype,
        quantization_config=bnb_cfg,
    )

    def _gen(prompt, max_new_tokens=256, temperature=0.2, top_p=0.9):
        inp = tok(prompt, return_tensors="pt").to(model.device)
        out = model.generate(**inp, max_new_tokens=max_new_tokens,
                             temperature=temperature, top_p=top_p,
                             do_sample=(temperature > 0))
        return tok.decode(out[0], skip_special_tokens=True)

    mode = "hf-8bit" if load_in_8bit else ("hf-4bit" if load_in_4bit else "hf-fp16")
    return {"backend": mode, "tokenizer": tok, "model": model, "generate": _gen}
