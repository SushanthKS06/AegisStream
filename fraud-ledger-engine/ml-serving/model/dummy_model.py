import hashlib
import random

class FraudScoringModel:
    """
    A mock machine learning model that simulates an XGBoost inference pipeline.
    To be production-like, this model:
    1. Evaluates incoming features (amount, 5-minute counts, 5-minute velocity).
    2. Runs a deterministic heuristic scoring engine based on risk rules.
    3. Employs a transaction-ID-based hash to add consistent, reproducible variance,
       ensuring re-evaluating the same transaction returns the exact same score.
    """

    def predict_probability(self, txn_id: str, amount: float, count_5m: int, velocity_5m: float, device_id: str) -> float:
        # Calculate a base risk score [0.0, 1.0]
        risk_score = 0.05

        # Rule 1: High Transaction Value
        if amount > 10000.0:
            risk_score += 0.40  # Massive transactions are flagged for verification
        elif amount > 2500.0:
            risk_score += 0.15

        # Rule 2: High Transaction Count (Velocity Check - Card-Testing Attack)
        if count_5m > 10:
            risk_score += 0.50  # Clear sign of botting / automation script
        elif count_5m > 5:
            risk_score += 0.20

        # Rule 3: High Absolute Velocity in 5-minute window
        if velocity_5m > 25000.0:
            risk_score += 0.35  # Rapid drainage of funds
        elif velocity_5m > 5000.0:
            risk_score += 0.10

        # Rule 4: Suspicious device fingerprints
        suspicious_devices = {"dev_unknown", "dev_emulator", "root_device"}
        if device_id.lower() in suspicious_devices:
            risk_score += 0.25

        # Rule 5: Deterministic Noise (Pseudo-random based on TxnID hash)
        # This acts like a model's residual error but is completely deterministic.
        hash_digest = hashlib.md5(txn_id.encode('utf-8')).hexdigest()
        hash_value = int(hash_digest[:4], 16) / 65535.0  # Normalize to [0, 1]
        noise = (hash_value - 0.5) * 0.1  # Map to [-0.05, 0.05]
        
        risk_score += noise

        # Apply sigmoid/clipping bounding to guarantee [0.0, 1.0]
        final_probability = min(max(risk_score, 0.0), 1.0)
        
        return round(final_probability, 4)
