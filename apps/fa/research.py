from __future__ import annotations
from typing import List, Dict, Any
import os, glob, yaml, json
from core.research import BaseResearcher, ResearchResult, SourceDoc

class FAResearcher(BaseResearcher):
    def search(self, question: str, prefixes: list[str]) -> ResearchResult:
        metrics_dir = self.settings.get("FA_METRICS_PATH", scope="namespace", namespace=self.namespace) \
                      or self.settings.get("FA_METRICS_PATH", scope="global", namespace=self.namespace) \
                      or "apps/fa/metrics"
        join_path = self.settings.get("FA_JOIN_GRAPH_PATH", scope="namespace", namespace=self.namespace) \
                    or self.settings.get("FA_JOIN_GRAPH_PATH", scope="global", namespace=self.namespace) \
                    or "apps/fa/join_graph.yaml"

        sources: List[SourceDoc] = []
        facts: Dict[str, Any] = {"fa": { "has_metrics": False, "has_joins": False, "metric_candidates": [], "joins": []}}

        # Load metrics
        if os.path.isdir(metrics_dir):
            metric_files = sorted(glob.glob(os.path.join(metrics_dir, "*.yaml")))
            for mf in metric_files:
                try:
                    with open(mf, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                    sources.append(SourceDoc(
                        source_type="internal_doc",
                        locator=mf, title=os.path.basename(mf),
                        content=json.dumps(data, ensure_ascii=False)
                    ))
                    # pick a few metric keys
                    for k, v in (data.get("metrics") or {}).items():
                        facts["fa"]["metric_candidates"].append({"key": k, "def": v})
                    facts["fa"]["has_metrics"] = True
                except Exception:
                    pass

        # Load join graph
        if os.path.exists(join_path):
            try:
                with open(join_path, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                sources.append(SourceDoc("internal_doc", join_path, "join_graph.yaml",
                                         json.dumps(data, ensure_ascii=False)))
                jlist = data.get("joins") or []
                for j in jlist:
                    facts["fa"]["joins"].append(j)
                if jlist:
                    facts["fa"]["has_joins"] = True
            except Exception:
                pass

        summary = "FA research: metrics loaded={} joins loaded={}".format(
            facts["fa"]["has_metrics"], facts["fa"]["has_joins"]
        )
        return ResearchResult(facts=facts, sources=sources, summary=summary)
