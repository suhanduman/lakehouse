"""oidc-login.py <başlangıç_url> <kullanıcı> <parola_ortam_değişkeni> <ca_dosyası|-> <beklenen_son_yol>
Tarayıcının yaptığını yapar: uygulamanın giriş adresinden başlar, Keycloak giriş formunu AD kullanıcısıyla doldurur,
yönlendirmeleri izler ve uygulamaya geri dönüldüğünü doğrular. Üretim kabulünde (ACC_FLOW=authcode) kullanılır;
parola komut satırına değil ortam değişkenine konur (adı verilir). CA dosyası router sertifikasının köküdür
(`-` = sistem kökleri). Başarıda son adresi basar, başarısızlıkta 1 ile çıkar."""
import html
import http.cookiejar
import os
import re
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request

start, user, pw_env, cafile, want = sys.argv[1:6]
ctx = ssl.create_default_context(cafile=None if cafile == "-" else cafile)
opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx),
                                     urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
# Trino web arayüzü tarayıcı başlığı olmayan isteklere 406 döner
opener.addheaders = [("User-Agent", "Mozilla/5.0 (lakehouse-kabul)"),
                     ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")]

r = opener.open(start, timeout=60)
m = re.search(r'<form[^>]+id="kc-form-login"[^>]+action="([^"]+)"', r.read().decode("utf-8", "replace"))
if not m:
    print(f"HATA Keycloak giriş formu yok (son adres: {r.geturl()})")
    sys.exit(1)
data = urllib.parse.urlencode({"username": user, "password": os.environ[pw_env], "credentialId": ""}).encode()
try:
    r = opener.open(html.unescape(m.group(1)), data=data, timeout=120)
    final, body = r.geturl(), r.read().decode("utf-8", "replace")
except urllib.error.HTTPError as e:
    print(f"HATA giriş HTTP {e.code} ({e.url})")
    sys.exit(1)
if urllib.parse.urlsplit(final).path.find(want) < 0:
    err = re.search(r'(?:kc-feedback-text|input-error[^"]*)"[^>]*>\s*([^<]{3,200})', body)
    print(f"HATA {user}: beklenen yol '{want}' değil -> {final}" + (f" ({err.group(1).strip()})" if err else ""))
    sys.exit(1)
print(f"OK {user} OIDC girişi -> {final}")
