import os, redis, json, hashlib

r = redis.Redis(host=os.getenv("REDIS_HOST","redis-cache"), port=int(os.getenv("REDIS_PORT","6379")), db=int(os.getenv("REDIS_DB","0")))

def key(ns, payload):
    payload_norm = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    h = hashlib.sha256(payload_norm.encode('utf-8')).hexdigest()
    return f"{ns}:{h}"

def invalidate_match(nk_partido_id: int):
    # roster partido
    k1 = key("api:v1:roster_partido", {"nk_partido_id": nk_partido_id})
    r.delete(k1)
    # historial podría venir filtrado por rango
    for rang in [
        {"desde": None, "hasta": None},
    ]:
        k2 = key("api:v1:historial_partidos", rang)
        r.delete(k2)
    print(f"OK: invalidated roster+historial for partido {nk_partido_id}")

if __name__ == "__main__":
    import sys
    invalidate_match(int(sys.argv[1]))
