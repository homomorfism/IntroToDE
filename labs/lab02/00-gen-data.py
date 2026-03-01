#!/usr/bin/env python3
import gzip
import json
import os
import random
import string
from datetime import datetime, timedelta, timezone

from faker import Faker
from tqdm import tqdm

fake = Faker()
random.seed(42)
Faker.seed(42)

OUT_DIR = "logs"
USERS_PATH = "users.json"
N_USERS = 2000
N_LOG_FILES = 100
MIN_FILE_MB = 2
MAX_FILE_MB = 5

# Error types for realism
ERROR_TYPES = [
    "TIMEOUT", "BAD_GATEWAY", "INTERNAL_SERVER_ERROR", "NOT_FOUND",
    "UNAUTHORIZED", "RATE_LIMITED", "DB_CONN_FAIL", "UPSTREAM_TIMEOUT",
    "INVALID_JSON", "SCHEMA_MISMATCH"
]

METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]
STATUS_BUCKETS = {
    "INFO":   [200, 201, 204, 304],
    "DEBUG":  [200, 200, 200, 204, 206, 304],
    "ERROR":  [400, 401, 403, 404, 409, 429, 500, 502, 503, 504]
}
PATHS = [
    "/api/v1/items", "/api/v1/items/{id}", "/api/v1/users",
    "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/orders/{id}",
    "/health", "/metrics", "/login", "/logout", "/search"
]
AGENTS = [fake.user_agent() for _ in range(200)]
REFS = [
    "-", "https://example.com", "https://search.example.com?q=" + fake.word(),
    "https://partner.example.net", "-"
]


def rand_request_path():
    p = random.choice(PATHS)
    if "{id}" in p:
        return p.replace("{id}", str(random.randint(1, 10_000)))
    return p


def rand_request_line():
    m = random.choice(METHODS)
    p = rand_request_path()
    q = ""
    if random.random() < 0.4:  # sprinkle some query params
        q = f"?page={random.randint(1,50)}&q={fake.word()}"
    return f"{m} {p}{q} HTTP/1.1"


def rand_req_id():
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=12))


def gen_users(n_users):
    # unique pool of IPs reused in logs
    ips = set()
    while len(ips) < n_users:
        ips.add(fake.ipv4_private())
    ips = list(ips)

    users = []
    for i, ip in enumerate(tqdm(ips, desc="Generating users")):
        users.append({
            "id": f"u{i:05d}",
            "profile": {
                "name": fake.name(),
                "username": fake.user_name(),
                "ip": ip,
                "tz": fake.timezone(),
                "locale": fake.locale()
            },
            "contact": {
                "email": fake.email(),
                "phone": fake.phone_number()
            },
            "preferences": {
                "lang": random.choice(["en", "es", "de", "fr", "ru", "zh"]),
                "marketing_opt_in": random.random() < 0.3,
                "theme": random.choice(["light", "dark"])
            },
            "devices": [
                {
                    "ua": random.choice(AGENTS),
                    "os": random.choice(["Linux", "Windows", "macOS", "Android", "iOS"])
                }
                for _ in range(random.randint(1, 3))
            ],
            "tags": random.sample(
                ["beta", "vip", "trial", "churn_risk", "staff", "partner"],
                k=random.randint(0, 3)
            )
        })
    return users


def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Generate users
    users = gen_users(N_USERS)
    ip_pool = [u["profile"]["ip"] for u in users]

    # Write users.json with timezone-aware UTC
    users_doc = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "meta": {"source": "faker", "version": 1},
        "users": users,
    }
    with open(USERS_PATH, "w", encoding="utf-8") as f:
        json.dump(users_doc, f, ensure_ascii=False, indent=2)

    # Time range for logs (last 7 days), timezone-aware UTC
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)
    total_seconds = (now - start).total_seconds()

    # Generate logs with per-file progress
    for idx in tqdm(range(N_LOG_FILES), desc="Generating log files"):
        target_bytes = random.randint(MIN_FILE_MB * 1024 * 1024,
                                      MAX_FILE_MB * 1024 * 1024)
        path = os.path.join(OUT_DIR, f"access-{idx:03d}.log.gz")
        written = 0
        with gzip.open(path, "wt", encoding="utf-8") as gz:
            while written < target_bytes:
                level = random.choices(
                    ["INFO", "DEBUG", "ERROR"], weights=[0.6, 0.25, 0.15]
                )[0]
                status = random.choice(STATUS_BUCKETS[level])
                ip = random.choice(ip_pool)

                # pick a random UTC timestamp in the window; %z prints +0000
                rand_sec = random.uniform(0, total_seconds)
                ts = (start + timedelta(seconds=rand_sec)).strftime("%d/%b/%Y:%H:%M:%S %z")

                req = rand_request_line()
                ref = random.choice(REFS)
                ua = random.choice(AGENTS)
                bytes_out = random.randint(200, 200_000)
                rid = rand_req_id()
                extra = f"level={level} request_id={rid}"
                if level == "ERROR":
                    et = random.choice(ERROR_TYPES)
                    extra += f" error_type=ERROR:{et}"

                line = (
                    f'{ip} - - [{ts}] "{req}" {status} {bytes_out} '
                    f'"{ref}" "{ua}" {extra}\n'
                )
                gz.write(line)
                written += len(line.encode("utf-8"))

    print(f"\n✅ Generated {N_LOG_FILES} gz logs in '{OUT_DIR}/' and '{USERS_PATH}'.")
    print("Tip: try zgrep/jq/parallel on these files for your lab tasks.")


if __name__ == "__main__":
    main()
