from datetime import date
from typing import List, Literal, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from .sites import df_json_records  # reuse JSON-safe conversion if needed later

router = APIRouter(prefix="/evaluate", tags=["evaluate"])

# ----- Models -----
class EvaluateRequest(BaseModel):
    site_att: str = Field(..., description="Site ATT identifier")
    input_date: date = Field(..., description="Reference input date")
    threshold: float = Field(0.05, ge=0.0, le=1.0, description="Delta threshold as fraction, default 0.05 (5%)")
    period: int = Field(7, ge=1, le=90, description="Period window in days")
    guard: int = Field(7, ge=0, le=90, description="Guard window in days")

MetricClass = Literal[
    "UpUp", "UpFlat", "UpDown",
    "FlatUp", "FlatFlat", "FlatDown",
    "DownUp", "DownFlat", "DownDown",
]

class MetricEvaluation(BaseModel):
    name: str
    before_mean: Optional[float]
    after_mean: Optional[float]
    last_mean: Optional[float]
    delta_after_before: Optional[float]
    delta_last_after: Optional[float]
    klass: Optional[MetricClass]
    verdict: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]

class EvaluateResponse(BaseModel):
    site_att: str
    input_date: date
    options: dict
    overall: Optional[Literal["Pass", "Fail", "Restored", "Inconclusive"]]
    metrics: List[MetricEvaluation]


# ----- Stub Evaluator -----
# For M4 scaffolding: return a deterministic placeholder using inputs.
# Later, compute actual ranges and means from DB selectors per RFP.

def classify(delta_ab: Optional[float], delta_la: Optional[float], thr: float) -> tuple[MetricClass, str]:
    def bucket(d: Optional[float]) -> str:
        if d is None:
            return "Flat"
        if d > thr:
            return "Up"
        if d < -thr:
            return "Down"
        return "Flat"

    a = bucket(delta_ab)
    b = bucket(delta_la)
    klass = f"{a}{b}"  # type: ignore
    # Verdict mapping (simplified): UpUp/UpFlat => Pass; Down* => Fail; *Up after Down => Restored
    if a == "Down" and b == "Up":
        verdict = "Restored"
    elif a == "Down" or b == "Down":
        verdict = "Fail"
    elif a == "Up" or b == "Up":
        verdict = "Pass"
    else:
        verdict = "Inconclusive"
    return klass, verdict


@router.post("")
def evaluate(req: EvaluateRequest) -> EvaluateResponse:
    # Placeholder metric set; wire real metrics later
    metric_names = [
        "CQI_DL", "CQI_UL", "Data_Traffic_GB", "Voice_Traffic_Min"
    ]
    metrics: List[MetricEvaluation] = []

    # Fake deltas using a simple seed to make it deterministic per input
    seed = (hash(req.site_att) ^ hash(req.input_date)) & 0xFFFF
    for i, name in enumerate(metric_names):
        d1 = ((seed >> (i * 3)) % 21 - 10) / 100.0  # -0.10..+0.10
        d2 = ((seed >> (i * 5)) % 21 - 10) / 100.0
        klass, verdict = classify(d1, d2, req.threshold)
        metrics.append(MetricEvaluation(
            name=name,
            before_mean=None,
            after_mean=None,
            last_mean=None,
            delta_after_before=d1,
            delta_last_after=d2,
            klass=klass,  # type: ignore[arg-type]
            verdict=verdict,  # type: ignore[arg-type]
        ))

    # Overall: worst-case precedence Fail > Restored > Pass > Inconclusive
    verdict_order = {"Fail": 3, "Restored": 2, "Pass": 1, "Inconclusive": 0}
    overall = sorted((m.verdict or "Inconclusive" for m in metrics), key=lambda v: verdict_order[v], reverse=True)[0]

    return EvaluateResponse(
        site_att=req.site_att,
        input_date=req.input_date,
        options={
            "threshold": req.threshold,
            "period": req.period,
            "guard": req.guard,
        },
        overall=overall,  # type: ignore[arg-type]
        metrics=metrics,
    )
