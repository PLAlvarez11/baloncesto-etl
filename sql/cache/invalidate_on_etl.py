import os, redis

r = redis.Redis(host=os.getenv("REDIS_HOST","redis-cache"), port=int(os.getenv("REDIS_PORT","6379")), db=int(os.getenv("REDIS_DB","0")))
for pattern in ["api:v1:*"]:
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=500)
        if keys:
            r.delete(*keys)
        if cursor == 0:
            break
print("OK: cache invalidated for patterns api:v1:*")
