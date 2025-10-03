from flask import Flask, request, jsonify
import asyncio
import json
import binascii
import requests
import aiohttp
import urllib3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
import like_pb2, like_count_pb2, uid_generator_pb2
from config import URLS_INFO, URLS_LIKE, FILES

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# ------------- Configurable tuning -------------
CONCURRENCY = 40
BATCH_SIZE = 200
BATCH_DELAY = 0.5
RETRIES = 1
REQUEST_TIMEOUT = 8
MAX_LIKES = 100   # 🔥 fixed: maximum 100 likes per request
# ------------------------------------------------


def load_tokens(server):
    try:
        filename = FILES.get(server, "token_bd.json")
        with open(f"tokens/{filename}", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def get_headers(token):
    return {
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 9; ASUS_Z01QD Build/PI)",
        "Connection": "Keep-Alive",
        "Accept-Encoding": "gzip",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
        "Expect": "100-continue",
        "X-Unity-Version": "2018.4.11f1",
        "X-GA": "v1 1",
        "ReleaseVersion": "OB50",
    }


def encrypt_message(data: bytes) -> str:
    cipher = AES.new(b'Yg&tc%DEuh6%Zc^8', AES.MODE_CBC, b'6oyZDr22E3ychjM%')
    return binascii.hexlify(cipher.encrypt(pad(data, AES.block_size))).decode()


def create_like(uid, region):
    m = like_pb2.like()
    m.uid, m.region = int(uid), region
    return m.SerializeToString()


def create_uid(uid):
    m = uid_generator_pb2.uid_generator()
    m.saturn_, m.garena = int(uid), 1
    return m.SerializeToString()


async def _send_single(session: aiohttp.ClientSession, token: str, url: str, data_hex: str):
    headers = get_headers(token)
    try:
        async with session.post(url, data=bytes.fromhex(data_hex), headers=headers, timeout=REQUEST_TIMEOUT) as r:
            if r.status == 200:
                return True
            return False
    except Exception:
        return False


async def multi(uid: str, server: str, url: str):
    """Send likes using max 100 tokens."""
    enc = encrypt_message(create_like(uid, server))
    tokens = load_tokens(server)
    if not tokens:
        return {"tokens_tried": 0, "success_count": 0}

    # flatten token list
    token_list = [t.get("token") if isinstance(t, dict) else t for t in tokens]
    token_list = [t for t in token_list if t]

    # 🔥 limit to 100 tokens only
    if len(token_list) > MAX_LIKES:
        import random
        token_list = random.sample(token_list, MAX_LIKES)

    total_tokens = len(token_list)
    success_count, tried = 0, 0

    connector = aiohttp.TCPConnector(limit_per_host=CONCURRENCY, force_close=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i in range(0, total_tokens, BATCH_SIZE):
            batch = token_list[i:i + BATCH_SIZE]

            sem = asyncio.Semaphore(CONCURRENCY)
            async def worker(token_str):
                nonlocal success_count, tried
                async with sem:
                    tried += 1
                    for attempt in range(RETRIES + 1):
                        ok = await _send_single(session, token_str, url, enc)
                        if ok:
                            success_count += 1
                            return True
                        await asyncio.sleep(0.1 + 0.05 * attempt)
                    return False

            tasks = [worker(tok) for tok in batch]
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(BATCH_DELAY)

    return {"tokens_tried": total_tokens, "success_count": success_count}


def get_info(enc_hex: str, server: str, token: str):
    try:
        url = URLS_INFO.get(server, "https://clientbp.ggblueshark.com/GetPlayerPersonalShow")
        r = requests.post(url, data=bytes.fromhex(enc_hex), headers=get_headers(token),
                          verify=False, timeout=REQUEST_TIMEOUT)
        p = like_count_pb2.Info()
        p.ParseFromString(r.content)
        return p
    except Exception:
        return None


@app.route("/like")
def like():
    uid = request.args.get("uid")
    server = request.args.get("server", "").upper()
    if not uid or not server:
        return jsonify(error="UID and server required"), 400

    tokens = load_tokens(server)
    if not tokens:
        return jsonify(error="No tokens found for this server"), 500

    enc_uid = encrypt_message(create_uid(uid))

    before, working_token = None, None
    for item in (tokens[: min(15, len(tokens))]):
        tok = item.get("token") if isinstance(item, dict) else item
        if not tok:
            continue
        before = get_info(enc_uid, server, tok)
        if before:
            working_token = tok
            break

    if not before:
        return jsonify(error="Player not found or no working token for info"), 500

    before_like = int(json.loads(MessageToJson(before)).get('AccountInfo', {}).get('Likes', 0))

    like_url = URLS_LIKE.get(server, "https://clientbp.ggblueshark.com/LikeProfile")

    try:
        loop = asyncio.get_running_loop()
        coro = multi(uid, server, like_url)
        fut = asyncio.run_coroutine_threadsafe(coro, loop)
        result = fut.result(timeout=300)
    except RuntimeError:
        result = asyncio.run(multi(uid, server, like_url))

    after_info = get_info(enc_uid, server, working_token)
    if not after_info:
        return jsonify(error="Could not fetch updated info"), 500

    after = json.loads(MessageToJson(after_info))
    after_like = int(after.get('AccountInfo', {}).get('Likes', 0))
    added = after_like - before_like

    return jsonify({
        "credits": "TOXIC XITERS",
        "likes_added": added,
        "likes_before": before_like,
        "likes_after": after_like,
        "player": after.get('AccountInfo', {}).get('PlayerNickname', ''),
        "uid": after.get('AccountInfo', {}).get('UID', 0),
        "status": 1 if added else 2,
        "tokens_tried": result.get("tokens_tried", 0),
        "success_count": result.get("success_count", 0)
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
