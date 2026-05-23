import time
import logging
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from model.dummy_model import FraudScoringModel

# Initialize structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ml-serving")

# Initialize FastAPI App
app = FastAPI(
    title="Real-Time Fraud ML Serving Engine",
    description="Low-latency inference service evaluating payment transactions for fraud.",
    version="1.0.0"
)

# Initialize scoring model
scoring_model = FraudScoringModel()

# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class PredictionRequest(BaseModel):
    txn_id: str = Field(..., description="Unique transaction ID", example="txn_892304")
    user_id: str = Field(..., description="Unique user/client ID", example="user_alice")
    amount: float = Field(..., gt=0.0, description="Transaction amount in USD", example=250.50)
    device_id: str = Field(..., description="Device hardware fingerprint", example="dev_chrome_win11")
    count_5m: int = Field(..., ge=0, description="Count of user's transactions in the last 5 minutes", example=2)
    velocity_5m: float = Field(..., ge=0.0, description="Sum of user's transaction amounts in the last 5 minutes", example=500.0)

class PredictionResponse(BaseModel):
    txn_id: str
    user_id: str
    fraud_probability: float
    decision: str = Field(..., description="Inference classification: 'APPROVE' or 'FLAG'")
    inference_time_ms: float

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post(
    "/predict",
    response_model=PredictionResponse,
    status_code=status.HTTP_200_OK,
    summary="Evaluate transaction risk score"
)
async def predict(request: PredictionRequest):
    """
    Evaluates an enriched transaction feature vector for fraud probability.
    Target latency: <10ms (leaves ample time for network/DB hops within the 50ms budget).
    """
    start_time = time.perf_counter()

    try:
        # Run inference
        probability = scoring_model.predict_probability(
            txn_id=request.txn_id,
            amount=request.amount,
            count_5m=request.count_5m,
            velocity_5m=request.velocity_5m,
            device_id=request.device_id
        )

        # Decision threshold boundary as per business logic specifications (Score > 0.85 = FRAUD/FLAG)
        decision = "FLAG" if probability > 0.85 else "APPROVE"

        # Calculate exact high-resolution elapsed time
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0

        logger.info(
            f"Txn: {request.txn_id} | User: {request.user_id} | "
            f"Amount: ${request.amount:.2f} | Probability: {probability:.4f} | "
            f"Decision: {decision} | Inference Time: {elapsed_ms:.3f}ms"
        )

        return PredictionResponse(
            txn_id=request.txn_id,
            user_id=request.user_id,
            fraud_probability=probability,
            decision=decision,
            inference_time_ms=round(elapsed_ms, 3)
        )

    except Exception as e:
        logger.error(f"Inference pipeline failed for transaction {request.txn_id}: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inference error: {str(e)}"
        )

@app.get("/health", status_code=status.HTTP_200_OK, summary="Service Health Check")
async def health():
    return {
        "status": "UP",
        "service": "ml-serving",
        "timestamp": time.time()
    }
