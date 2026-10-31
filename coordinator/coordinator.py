import csv
import hashlib
import io
import json
import os
import pickle
import re
import socket
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from itertools import combinations

import boto3
from kazoo.client import KazooClient
from kazoo.exceptions import (BadVersionError, ConnectionLoss, NodeExistsError,
                              OperationTimeoutError, SessionExpiredError)

ZK_HOSTS = os.environ.get("ZK_HOSTS", "zk1:2181,zk2:2181,zk3:2181")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://host.docker.internal:9100")
ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
SECRET_KEY = os.environ.get("S3_SECRET_KEY", "rustfsadmin")
BUCKET = "hh-lake"
CSV_KEY = "raw/it_vacancies_2024.csv"
REFERENCE_KEY = "results/mapreduce/skills_2.csv"
CHECKPOINT_KEY = "zk/checkpoint.pkl"
ROLE = "Программист, разработчик"
NAME_RE = re.compile(r"(^|[^0-9])1\s?[cс]")
CHUNK = int(os.environ.get("CHUNK", 8 * 1024 * 1024))
SESSION_TIMEOUT = int(os.environ.get("SESSION_TIMEOUT", "10"))
FENCING = os.environ.get("FENCING", "0") == "1"
WORKERS = int(os.environ.get("WORKERS", "4"))
BATCH = int(os.environ.get("BATCH", "4"))
BATCH_DELAY = float(os.environ.get("BATCH_DELAY", "0"))
COORD_ID = os.environ.get("COORD_ID", socket.gethostname())
RUN_NAME = os.environ.get("RUN_NAME", "run")
EXPECTED = {"variant": 20953, "with_skills": 13452, "pairs": 32298}

ROOT = "/calc"
LEADER = ROOT + "/leader"
EPOCH = ROOT + "/epoch"
CHECKPOINT = ROOT + "/checkpoint"
RESULT = ROOT + "/result"

csv.field_size_limit(sys.maxsize)


def log(message):
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{stamp} [{COORD_ID}] {message}"
    print(line, flush=True)
    os.makedirs("logs", exist_ok=True)
    with open(f"logs/{RUN_NAME}-{COORD_ID}.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")


def s3_client():
    return boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY, region_name="us-east-1")


def zk_call(func, *args, **kwargs):
    for _ in range(60):
        try:
            return func(*args, **kwargs)
        except (ConnectionLoss, SessionExpiredError, OperationTimeoutError):
            time.sleep(1)
    return func(*args, **kwargs)


def norm_skill(s):
    return s.strip(" ").lower().replace("1c", "1с")


def last_boundary(buf):
    parts = buf.split(b'"')
    offset, cut = 0, 0
    for i, p in enumerate(parts):
        if i % 2 == 0:
            j = p.rfind(b"\n")
            if j != -1:
                cut = offset + j + 1
        offset += len(p) + 1
    return cut


class Reader:
    def __init__(self, s3, state=None):
        self.s3 = s3
        self.size = s3.head_object(Bucket=BUCKET, Key=CSV_KEY)["ContentLength"]
        state = state or {"pos": 0, "carry": b"", "idx": 0, "cols": None}
        self.pos = state["pos"]
        self.carry = state["carry"]
        self.idx = state["idx"]
        self.cols = state["cols"]

    def state(self):
        return {"pos": self.pos, "carry": self.carry, "idx": self.idx, "cols": self.cols}

    def next(self):
        while self.pos < self.size:
            end = min(self.pos + CHUNK, self.size) - 1
            body = self.s3.get_object(Bucket=BUCKET, Key=CSV_KEY,
                                      Range=f"bytes={self.pos}-{end}")["Body"].read()
            self.pos = end + 1
            buf = self.carry + body
            cut = len(buf) if self.pos >= self.size else last_boundary(buf)
            if cut == 0:
                self.carry = buf
                continue
            data, self.carry = buf[:cut], buf[cut:]
            text = data.decode("utf-8-sig")
            if self.cols is None:
                header_end = text.index("\n") + 1
                header = next(csv.reader(io.StringIO(text[:header_end], newline="")))
                self.cols = {name: i for i, name in enumerate(header)}
                text = text[header_end:]
            self.idx += 1
            return self.idx - 1, text
        return None


def mapper(task):
    text, cols = task
    pairs = Counter()
    total = variant = with_skills = bad = 0
    need = max(cols["role"], cols["name"], cols["key_skills"])
    for row in csv.reader(io.StringIO(text, newline="")):
        total += 1
        if len(row) <= need:
            bad += 1
            continue
        name = row[cols["name"]].lower()
        if row[cols["role"]] != ROLE or not NAME_RE.search(name):
            continue
        if "битрикс" in name or "bitrix" in name:
            continue
        variant += 1
        raw = row[cols["key_skills"]]
        if not raw:
            continue
        skills = sorted({norm_skill(s) for s in raw.split(";") if s.strip(" ")})
        if not skills:
            continue
        with_skills += 1
        pairs.update(combinations(skills, 2))
    return pairs, total, variant, with_skills, bad


def write_result(counter, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["skills", "count"])
        for key, cnt in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])):
            w.writerow([" || ".join(key), cnt])


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest().upper()


def reference_sha(s3):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=REFERENCE_KEY)["Body"]
        h = hashlib.sha256()
        for block in iter(lambda: body.read(1 << 20), b""):
            h.update(block)
        return h.hexdigest().upper()
    except Exception:
        return None


def ensure_nodes(client):
    client.ensure_path(ROOT)
    for path, value in ((EPOCH, b"0"), (CHECKPOINT, b""), (RESULT, b"")):
        try:
            client.create(path, value)
        except NodeExistsError:
            pass


def acquire_leadership(client):
    while True:
        try:
            client.create(LEADER, COORD_ID.encode(), ephemeral=True)
            return
        except NodeExistsError:
            event = threading.Event()
            if client.exists(LEADER, watch=lambda e: event.set()) is None:
                continue
            event.wait()


def next_epoch(client):
    while True:
        data, stat = zk_call(client.get, EPOCH)
        value = int(data) + 1
        try:
            zk_call(client.set, EPOCH, str(value).encode(), version=stat.version)
            return value
        except BadVersionError:
            continue


def load_checkpoint(client, s3):
    data, _ = zk_call(client.get, CHECKPOINT)
    if not data:
        return None
    pointer = json.loads(data)
    blob = s3.get_object(Bucket=BUCKET, Key=pointer["key"])["Body"].read()
    return pickle.loads(blob)


def save_checkpoint(client, s3, reader, counter, stat, epoch):
    blob = pickle.dumps({"reader": reader.state(), "counter": counter, "stat": stat})
    s3.put_object(Bucket=BUCKET, Key=CHECKPOINT_KEY, Body=blob)
    pointer = json.dumps({"key": CHECKPOINT_KEY, "chunks": stat["chunks"], "epoch": epoch})
    zk_call(client.set, CHECKPOINT, pointer.encode())


def compute(client, s3, reader, counter, stat, epoch):
    with ProcessPoolExecutor(WORKERS) as ex:
        while True:
            batch = []
            for _ in range(BATCH):
                item = reader.next()
                if item is None:
                    break
                batch.append(item)
            if not batch:
                break
            futures = [ex.submit(mapper, (text, reader.cols)) for _, text in batch]
            for fut in futures:
                pairs, total, variant, with_skills, bad = fut.result()
                counter.update(pairs)
                stat["total"] += total
                stat["variant"] += variant
                stat["with_skills"] += with_skills
                stat["bad"] += bad
                stat["chunks"] += 1
            save_checkpoint(client, s3, reader, counter, stat, epoch)
            log(f"чекпоинт: обработано чанков {stat['chunks']}, позиция {reader.pos} из {reader.size}")
            if BATCH_DELAY:
                time.sleep(BATCH_DELAY)


def publish(client, s3, counter, stat, epoch, expected_version):
    path = f"/tmp/skills_2_epoch{epoch}.csv"
    write_result(counter, path)
    sha = file_sha256(path)
    ref = reference_sha(s3)
    match = "да" if (stat["variant"] == EXPECTED["variant"]
                     and stat["with_skills"] == EXPECTED["with_skills"]
                     and len(counter) == EXPECTED["pairs"]) else "нет"
    log(f"контроль: вакансий варианта {stat['variant']}, с навыками {stat['with_skills']}, "
        f"уникальных пар {len(counter)}, сумма частот {sum(counter.values())}, "
        f"совпало с ДЗ 3 по числам: {match}")
    log(f"контроль: sha256 результата {sha}")
    log(f"контроль: sha256 эталона ДЗ 3 {ref}, совпал: {'да' if ref == sha else 'нет'}")
    current_epoch = int(zk_call(client.get, EPOCH)[0])
    log(f"публикация: моя эпоха {epoch}, текущая эпоха в ZooKeeper {current_epoch}, защита {'включена' if FENCING else 'выключена'}")
    key = f"zk/result/skills_2_epoch{epoch}.csv"
    s3.upload_file(path, BUCKET, key)
    payload = json.dumps({"epoch": epoch, "leader": COORD_ID, "key": key, "sha256": sha,
                          "variant": stat["variant"], "with_skills": stat["with_skills"],
                          "pairs": len(counter)}, ensure_ascii=False).encode()
    version = expected_version if FENCING else -1
    try:
        znode = zk_call(client.set, RESULT, payload, version=version)
    except BadVersionError:
        log(f"запись отклонена: BadVersionError (эпоха {epoch}, ожидалась версия указателя {expected_version})")
        s3.delete_object(Bucket=BUCKET, Key=key)
        return False
    log(f"результат сохранен: эпоха {epoch}, объект {key}, версия указателя {znode.version}")
    return True


def reset():
    client = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
    client.start(timeout=30)
    if client.exists(ROOT):
        client.delete(ROOT, recursive=True)
    client.stop()
    client.close()
    s3 = s3_client()
    listing = s3.list_objects_v2(Bucket=BUCKET, Prefix="zk/")
    for obj in listing.get("Contents", []):
        s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
    print("состояние координатора сброшено")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "reset":
        reset()
        return
    s3 = s3_client()
    client = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
    client.add_listener(lambda state: log(f"состояние сессии ZooKeeper: {state}"))
    client.start(timeout=30)
    log(f"запущен, тайм-аут сессии {SESSION_TIMEOUT} с, FENCING={int(FENCING)}, процессов {WORKERS}")
    ensure_nodes(client)
    log("ожидаю лидерства")
    acquire_leadership(client)
    epoch = next_epoch(client)
    log(f"лидерство получено, эпоха {epoch}")
    data, rstat = zk_call(client.get, RESULT)
    if data:
        log("результат уже опубликован, завершаю работу")
        client.stop()
        client.close()
        return
    expected_version = rstat.version
    checkpoint = load_checkpoint(client, s3)
    if checkpoint:
        counter = checkpoint["counter"]
        stat = checkpoint["stat"]
        reader = Reader(s3, checkpoint["reader"])
        log(f"продолжаю с чекпоинта: обработано чанков {stat['chunks']}, позиция {reader.pos}")
    else:
        counter = Counter()
        stat = {"total": 0, "variant": 0, "with_skills": 0, "bad": 0, "chunks": 0}
        reader = Reader(s3)
        log("чекпоинта нет, начинаю расчёт с начала")
    compute(client, s3, reader, counter, stat, epoch)
    log(f"расчёт завершён, чанков {stat['chunks']}")
    publish(client, s3, counter, stat, epoch, expected_version)
    client.stop()
    client.close()


if __name__ == "__main__":
    main()