#!/usr/bin/env markdown
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
- **settings.py**: مصدر الحقيقة للإعدادات من DB (namespace `dw::common`).
- **fts.py**: يبني WHERE للبحث النصّي حسب الإعدادات.
- **explain.py**: يبني rationale نظيف للمستخدم + صفحة HTML `/dw/admin/explain`.

---

## STRUCTURE & OWNERSHIP (خطّ فاصل واضح)

### 1) `core/` (النواة – Project-Agnostic)
- يضم **لبنات عامة قابلة لإعادة الاستخدام** بين التطبيقات:  
  *settings infra*, *shared DB/session factory*, *logging*, *sql utils*, *validators*, *rate grammar parsing*, *generic explain/research*.
- **ممنوع** وضع منطق خاص بتطبيق/دومين بعينه داخل `core/`.
- **قاعدة اعتماد:** `apps/*` يجوز لها استيراد `core/`، لكن `core/` **لا** يستورد من `apps/*`.

### 2) `apps/<app_name>/` (منطق خاص بالتطبيق)
- كل ما يخص الدومين أو التطبيق بعينه (مثل `apps/dw` لـ DocuWare).
- يحقّ لـ `apps/dw` استخدام ما في `core/` + الإعدادات من الـ DB عبر واجهة موحّدة.

### 3) Table-Scoped Modules (لكل جدول مجلّد/ملفات)
منطق جدول بعينه يجب أن يعيش في ملفاته/مجلّده داخل التطبيق:
```
apps/dw/contracts/               # نطاق "Contract"
  builder.py                     # بُناة SQL الخاصة بالجدول (filters/group/order)
  fts_rules.py                   # قواعد FTS/Equality الخاصة بالجدول (إن وُجدت)
  metrics.py                     # تعريفات قياسات الجدول (gross/avg/… إذا كانت خاصة)
  tests/                         # golden/asserts الخاصة بالجدول
```
- **قاعدة اعتماد:** وحدات الجدول لا تستورد جداول أخرى مباشرة؛ لو احتجنا مشاركة منطق عام نضعه في `apps/dw/common/` أو `core/`.
- **التسمية:** أسماء واضحة وصريحة تعكس الدور: `*_builder.py`, `*_rules.py`, `*_metrics.py`.

### 4) قواعد الاستيراد والاعتمادية
- `core/` → لا يعتمد على `apps/*`.  
- `apps/*` → يعتمد على `core/` + الإعدادات من DB.  
- وحدات الجدول → تعتمد على واجهات `apps/dw/settings.py` و `apps/dw/fts.py` و utils عامة فقط.
- **لا hardcode** لأي setting؛ استخدم دائمًا Settings من DB (`/admin/settings/bulk`).

### 5) الاختبارات و Golden
- لكل جدول، اجعل Golden/Asserts الخاصة به تحت `apps/dw/<table>/tests/` أو أضف حالات مركّزة داخل `docs/golden/*.yaml` مع تسمية واضحة.  
- Golden يجب أن تؤكد:  
  - وجود WHERE/ORDER/GROUP BY المتوقعين،  
  - اتجاه ORDER BY (ASC/ DESC) الصحيح عند lowest/top،  
  - عدم السقوط في fallback.

### 6) التكوين (Configuration)
- مصدر الحقيقة: DB settings `dw::common`.  
- مفاتيح رئيسية:  
  - `DW_FTS_ENGINE` (`like` افتراضيًا)،  
  - `DW_FTS_COLUMNS` (أعمدة البحث النصّي لكل جدول)،  
  - `DW_EXPLICIT_FILTER_COLUMNS` (أعمدة مساواة)،  
  - `DW_ENUM_SYNONYMS` (مرادفات enums مثل REQUEST_TYPE)،  
  - `DW_CONTRACT_TABLE`, `DW_DATE_COLUMN`.

### 7) أسلوب العمل (Workflow)
- أي تعديل وظيفي = **Branch** واضح + **Diff** بسيط + **Golden** تمنع الارتداد + **Explain** مختصر للتغييرات.
- التعليقات داخل الشيفرة **بالإنجليزية فقط**.
