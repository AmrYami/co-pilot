#!/usr/bin/env markdown
# نطاق DocuWare (DW)

## جدول Contract (أمثلة لأعمدة رئيسية)
- CONTRACT_ID, REQUEST_ID, CONTRACT_ID_COUNTER
- ENTITY, ENTITY_NO, OWNER_DEPARTMENT, DEPARTMENT_OUL
- CONTRACT_OWNER, REQUESTER
- CONTRACT_STATUS, REQUEST_TYPE
- CONTRACT_VALUE_NET_OF_VAT, VAT
- REQUEST_DATE, START_DATE, END_DATE
- CONTRACT_STAKEHOLDER_1..8
- REPRESENTATIVE_EMAIL, LEGAL_NAME_OF_THE_COMPANY
- CONTRACT_SUBJECT, CONTRACT_PURPOSE

> **Schema reference**: راجع `schema.sql` في جذر المشروع (إن وُجد) أو المسار الذي رفعته.

## Gross measure (افتراضي):
```sql
NVL(CONTRACT_VALUE_NET_OF_VAT,0) 
  + CASE WHEN NVL(VAT,0) BETWEEN 0 AND 1
         THEN NVL(CONTRACT_VALUE_NET_OF_VAT,0) * NVL(VAT,0)
         ELSE NVL(VAT,0) END
```

## أسئلة شائعة (نماذج)
- “Show contracts where REQUEST TYPE = Renewal”
- “list all contracts has it or home care (FTS)”
- “For ENTITY_NO = 'E-123', total and count by CONTRACT_STATUS”
