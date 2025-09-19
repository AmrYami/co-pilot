"""DocuWare-specific clarifier helper for follow-up questions."""

from __future__ import annotations

from typing import List

from core.model_loader import load_llm

SYS = (
    "You're a concise assistant that writes at most 2 short yes/no clarifying questions "
    "to help convert a business request into SQL over a single table named Contract. "
    "Each bullet must end with a question mark."
)

EXAMPLE = (
    "User: top stakeholders by contract value\n"
    "Assistant:\n"
    "- Should we use REQUEST_DATE or END_DATE for the time filter?\n"
    "- Do you want gross contract value (net + VAT)?"
)


def propose_clarifying_questions(user_question: str) -> List[str]:
    clar = load_llm("clarifier")
    if not clar:
        return [
            "Which date field should we use (REQUEST_DATE or END_DATE) and what time window?",
            "Should value be NET, VAT, or NET+VAT (gross)?",
        ]

    handle = clar.get("handle")

    prompt = f"{SYS}\n\n{EXAMPLE}\n\nUser: {user_question}\nAssistant:\n"
    if handle is not None:
        text = handle.generate(prompt, max_new_tokens=96, temperature=0.2, top_p=0.9)
    else:
        tokenizer = clar.get("tokenizer")
        model = clar.get("model")
        if tokenizer is None or model is None:
            text = ""
        else:
            import torch

            device = getattr(model, "device", None)
            if device is None:
                try:
                    device = next(model.parameters()).device
                except Exception:
                    device = None
            inputs = tokenizer(prompt, return_tensors="pt")
            if device is not None:
                inputs = {k: v.to(device) for k, v in inputs.items()}
            with torch.inference_mode():
                outputs = model.generate(
                    **inputs,
                    max_new_tokens=96,
                    do_sample=True,
                    temperature=0.2,
                    top_p=0.9,
                    pad_token_id=tokenizer.eos_token_id,
                )
            text = tokenizer.decode(outputs[0], skip_special_tokens=True)

    tail = text.split("Assistant:")[-1].strip()
    lines = []
    for raw_line in tail.splitlines():
        cleaned = raw_line.strip()
        if not cleaned:
            continue
        cleaned = cleaned.lstrip("-• ").strip()
        if not cleaned.endswith("?"):
            cleaned = f"{cleaned}?"
        lines.append(cleaned)

    if lines:
        return lines[:2]

    return [
        "Should we use REQUEST_DATE or END_DATE for the time filter?",
        "Do you need gross contract value (net + VAT)?",
    ]

