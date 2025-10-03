from flask import Flask, request, jsonify
import asyncio, json, binascii, requests, aiohttp, urllib3
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import DecodeError
import like_pb2, like_count_pb2, uid_generator_pb2
from config import URLS_INFO, URLS_LIKE, FILES

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__)


# ================= TOKEN LOADER =================
def load_tokens(server):
    files = FILES
    return json.load(open(f"tokens/{files.get(server, 'token_bd.json')}"))


# ================= REQUEST HEADERS =================
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


# ================= ENCRYPTION =================
def encrypt_message(data):
    cipher = AES.new(b'Yg&tc%DEuh6%Zc^8', AES.MODE_CBC, b'6oyZDr22E3ychjM%')
    return binascii.hexlify(cipher.encrypt(pad(data, AES.block_size))).decode()


# ================= MESSAGE BUILDERS =================
def create_like(uid, region):
    m = like_pb2.like()
    m.uid, m.region = int(uid), region
    return m.SerializeToString()


def create_uid(uid):
    m = uid_generator_pb2.uid_generator()
    m.saturn_, m.garena = int(uid), 1
    return m.SerializeToString()


# ================= LIKE SENDER =================
async def send(token, url, data):
    headers = get_headers(token)
    async with aiohttp.ClientSession() as s:
        try:
            async with s.post(url, data=bytes.fromhex(data), headers=headers) as r:
                return await r.text() if r.status == 200 else None
        except:
            return None


# ================= MULTI TOKEN HANDLER =================
async def multi(uid, server, url):
    enc = encrypt_message(create_like(uid, server))
    tokens = load_tokens(server)
    results = []

    # batch system to avoid blocking
    batch_size = 50
    for i in range(0, len(tokens), batch_size):
        batch = tokens[i:i + batch_size]
        responses = await asyncio.gather(
            *[send(t['token'], url, enc) for t in batch],
            return_exceptions=True
        )
        results.extend(responses)
        await asyncio.sleep(0.5)  # short delay

    return results


# ================= GET PLAYER INFO =================
def get_info(enc, server, token):
    urls = URLS_INFO
    r = requests.post(
        urls.get(server, "https://clientbp.ggblueshark.com/GetPlayerPersonalShow"),
        data=bytes.fromhex(enc), headers=get_headers(token), verify=False
    )
    try:
        p = like_count_pb2.Info()
        p.ParseFromString(r.content)
        return p
    except DecodeError:
        return None


# ================= MAIN LIKE API =================
@app.route("/like")
def like():
    uid, server = request.args.get("uid"), request.args.get("server", "").upper()
    if not uid or not server:
        return jsonify(error="UID and server required"), 400

    tokens = load_tokens(server)
    enc = encrypt_message(create_uid(uid))

    # find working token
    before, tok = None, None
    for t in tokens:
        before = get_info(enc, server, t["token"])
        if before:
            tok = t["token"]
            break

    if not before:
        return jsonify(error="Player not found"), 500

    before_like = int(json.loads(MessageToJson(before)).get('AccountInfo', {}).get('Likes', 0))
    urls = URLS_LIKE

    # send likes using all tokens
    asyncio.run(multi(uid, server, urls.get(server, "https://clientbp.ggblueshark.com/LikeProfile")))

    after = json.loads(MessageToJson(get_info(enc, server, tok)))
    after_like = int(after.get('AccountInfo', {}).get('Likes', 0))

    return jsonify({
        "credits": "TOXIC XITERS",
        "likes_added": after_like - before_like,
        "likes_before": before_like,
        "likes_after": after_like,
        "player": after.get('AccountInfo', {}).get('PlayerNickname', ''),
        "uid": after.get('AccountInfo', {}).get('UID', 0),
        "status": 1 if after_like - before_like else 2,
    })


# ================= RUN SERVER =================
if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
