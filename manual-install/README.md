# manual-install/ — chart'tan üretilen tek-dosya manuel kurulum

**Tek doğruluk kaynağı `../chart/`'tır.** Bu klasör elle bakımı yapılan bir
manifest seti DEĞİLDİR — `render.sh`, `helm template` render'ını tek bir
dosyaya yazan bir kolaylıktır; air-gapped kümeler, Helm binary'sinin
kurulamadığı ortamlar veya GitOps tool'unuzun (ArgoCD/Flux) düz manifest
beklediği senaryolar için vardır.

Normal/önerilen kurulum yolu **`helm install`**'dır (bkz. kök `README.md` →
"HELM İLE KURULUM"). Bu klasörü yalnızca Helm kullanamıyorsanız kullanın.

## İçerik

```
manual-install/
├── README.md         (bu dosya)
├── render.sh          chart/ -> manifests/lakehouse-<ns>.yaml üretir
└── manifests/
    └── lakehouse-example.yaml   (commit'li, `example` namespace'i için üretilmiş örnek)
```

`manifests/lakehouse-example.yaml` dosyasının başında bir üretim notu vardır:
**GENERATED from `chart/` — do not edit; edit the chart and re-run
`render.sh` instead.** Chart'ta bir değişiklik yaptığınızda bu dosyayı elle
güncellemeyin — `render.sh`'i yeniden çalıştırıp çıktıyı yeniden commit edin.

## Kullanım

### 1. Kendi namespace/values'ınız için yeniden üretin (opsiyonel)

`manifests/lakehouse-example.yaml`, `example` namespace'i + `chart/values-prod.example.yaml`
için üretilmiştir. Farklı bir namespace veya kendi values dosyanız için:

```bash
# Kendi values dosyanızı chart/values-prod.example.yaml'dan türetin, sonra:
./render.sh <namespace> <values-dosyanız.yaml>
# örn: ./render.sh example ../chart/values-prod.example.yaml
```

### 2. Kurulum sırası

```bash
# (1) Cluster-scoped OLM operatör önkoşulları (helm kurulumuyla aynı):
oc apply -f ../operators/
oc get csv -A | grep -E "cert-manager|external-secrets|cloudnative-pg|strimzi|spark-operator|keycloak"
# Her satır PHASE = Succeeded olana kadar bekleyin (bkz. ../operators/README.md).

# (2) Secret'ları oluşturun (helm kurulumundaki "Secret'ları hazırlama" adımıyla
#     birebir aynı — bkz. kök README.md § HELM İLE KURULUM veya tools/README.md
#     § "Secret'ları hazırlama"):
oc -n example create secret generic s3-credentials \
  --from-literal=access-key-id=... --from-literal=secret-access-key=... \
  --from-literal=endpoint=... --from-literal=region=...
oc -n example create secret generic oidc-credentials --from-literal=issuer-url=... \
  --from-literal=trino-client-id=... --from-literal=trino-client-secret=...
# ... (tam liste için üretilmiş manifestin içindeki Secret nesnelerine ve
# tools/README.md'ye bakın)

# (3) Üretilen manifesti uygulayın:
oc apply -f manifests/lakehouse-example.yaml
# (veya kendi namespace'iniz için yeniden ürettiyseniz onun çıktısını)

# (4) Doğrulayın:
oc -n example get pods,cluster,kafka,kafkaconnect
watch oc -n example get pods
```

**Rollback:** Helm kullanmadığınız için `helm rollback` yoktur — önceki
sürümün `manifests/lakehouse-<ns>.yaml`'ını git geçmişinden alıp tekrar
`oc apply -f` etmeniz veya tek tek `oc rollout undo` çalıştırmanız gerekir.

## Sınırlamalar

- Bu dosya CNPG/Strimzi/Keycloak Operator/Spark Operator gibi operatörlerin
  ürettiği (`../operators/`) CRD'lere bağımlıdır — operatörler kurulu değilse
  render'daki CR'lar `Pending`/hata durumunda kalır.
- Helm'in sağladığı `--dry-run`, `helm diff`, `helm rollback`, release
  history gibi kolaylıklar yoktur; bu klasör bilinçli olarak minimaldir.
- Chart-dışı hâlâ-canlı araçlar (`../tools/` — scaffold CLI, Spark job'lar,
  validator) bu klasörün kapsamı dışındadır; onlar için `../tools/README.md`.
