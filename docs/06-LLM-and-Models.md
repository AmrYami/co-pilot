# الموديلات (LLM) والبنية

## Planner / Clarifier (اختياري)
- Planner: SQLCoder-70B EXL2 (عند الحاجة للتخطيط الثقيل)
- Clarifier: Meta-Llama-3.1-8B (تصنيف/استخراج نوايا/Rate parsing)

## ملاحظات تشغيل
- ExLlamaV2/flash-attn: بالفعل لديك ضبط بيئي (TORCH_CUDA_ARCH_LIST, RESERVE_VRAM_GB …).  
- يمكن تعطيل الـ dynamic generator (force base) عند ظهور مشاكل.

## Caching/Serving
- Split عبر GPUs (5090 + 3060) وفق ENV.
- Fallback إلى CPU عندما يلزم.
