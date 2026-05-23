import time
import logging
import redis

logger = logging.getLogger("stream-processor.redis")

class RedisFeatureStore:
    """
    Production-grade sliding-window feature store using Redis Sorted Sets (ZSET).
    Tracks transaction frequency and absolute spending velocity for each user ID.
    
    Data Structure:
      Key: `txns:{user_id}`
      Member: `"{txn_id}:{amount}"`
      Score: Unix epoch timestamp (with millisecond float precision)
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0):
        # Create a connection pool to maximize reuse and avoid reconnection costs
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=True, # Decode byte strings to Python strings automatically
            socket_timeout=2.0,
            socket_keepalive=True
        )
        self.r = redis.Redis(connection_pool=self.pool)
        logger.info(f"Connected to Redis Feature Store at {host}:{port}")

    def get_and_update_velocity(self, user_id: str, txn_id: str, amount: float, window_seconds: int = 300) -> tuple[int, float]:
        """
        Atomically records a new transaction and computes sliding-window features
        (count and total spending velocity) for the specified user within a time frame.
        
        Returns:
            Tuple of (count_5m, velocity_5m)
        """
        key = f"txns:{user_id}"
        now = time.time()
        cutoff = now - window_seconds

        # Unique value containing txn_id and amount
        member_val = f"{txn_id}:{amount}"

        try:
            # Use Redis Pipeline to run all commands in a single round-trip (RTT = 1)
            pipe = self.r.pipeline()
            
            # 1. Add current transaction with current timestamp score
            pipe.zadd(key, {member_val: now})
            
            # 2. Prune outdated transactions older than cutoff (e.g., >5 minutes old)
            pipe.zremrangebyscore(key, "-inf", cutoff)
            
            # 3. Fetch all transaction records remaining in the sliding window
            pipe.zrange(key, 0, -1)
            
            # 4. Refresh TTL to auto-prune idle keys and prevent memory leaks (10 min TTL)
            pipe.expire(key, window_seconds * 2)
            
            # Execute pipeline
            _, _, active_members, _ = pipe.execute()

            # Parse members and compile aggregates
            count = len(active_members)
            velocity = 0.0

            for member in active_members:
                try:
                    # Format: "txn_id:amount"
                    parts = member.split(":")
                    if len(parts) >= 2:
                        velocity += float(parts[1])
                except (ValueError, IndexError) as parse_err:
                    logger.warning(f"Failed to parse velocity entry '{member}': {parse_err}")

            return count, round(velocity, 4)

        except redis.RedisError as e:
            logger.error(f"Redis feature store error for user {user_id}: {str(e)}. Falling back to default metrics.")
            # Fail-safe fallback to prevent blocking transaction ingestion if Redis is temporarily offline
            return 1, amount
