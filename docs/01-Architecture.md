# البنية المعمارية (Architecture)

## بنية المستودع (مختصر)
```
longchain/
  apps/dw/
    app.py              # Flask blueprint (DW endpoints)
    settings.py         # DB-backed settings access (namespace dw::common)
    fts.py              # FTS builder (LIKE / Oracle Text), AND/OR tokens
    explain.py          # user-facing rationale builder (HTML+text)
    ...                 # research/rate/golden helpers
docs/
  ...                   # هذه الملفات
scripts/
  export_context.py     # تصدير حالة settings/examples/rules/patches -> docs/state
```

## كيف تُبنى الإجابة؟
1. **Input**: `{question, full_text_search, ...}` إلى `/dw/answer`.
2. **Settings**: قراءة `DW_FTS_COLUMNS`, `DW_FTS_ENGINE`, `DW_EXPLICIT_FILTER_COLUMNS`, `DW_ENUM_SYNONYMS`, `DW_DATE_COLUMN`.
3. **FTS**: tokens AND/OR → LIKE على الأعمدة المُحدّدة (أو CONTAINS لو فعّلنا Oracle Text).
4. **Equality**: `COLUMN = VALUE` فقط لو العمود ضمن `DW_EXPLICIT_FILTER_COLUMNS` (مع ci/trim).  
   - `REQUEST_TYPE` تُوسَّع عبر مرادفات equals/prefix/contains.
5. **Group/Metric**: أبعاد مثل `DEPARTMENT_OUL`, `ENTITY_NO`، ومقياس gross الافتراضي.
6. **Order**: DESC افتراضيًا (REQUEST_DATE). `lowest/bottom` ⇒ ASC. دعم `FETCH FIRST :N`.
7. **Explain**: rationale نصّي + بنيوي (explain_struct).
8. **Rate**: `/dw/rate` يطبّق تعليق المستخدم فورًا (fts/eq/group/order/top-bottom).

## ما هو `apps/dw/...`؟
- **settings.py**: مصدر الحقيقة للإعدادات من DB.
- **fts.py**: يبني WHERE للبحث النصّي حسب الإعدادات.
- **explain.py**: يبني rationale نظيف للمستخدم + صفحة HTML `/dw/admin/explain`.
