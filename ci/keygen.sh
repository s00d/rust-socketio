#!/bin/sh
# Test certificates for the secure engine.io server (ci/engine-io-secure.js):
#   ./keygen.sh <DOMAIN> <IP>      e.g. ./keygen.sh node-engine-io-secure 127.0.0.1
# Files land in ci/cert (gitignored). Existing files are kept, so a CA you trust stays valid.
# Tests trust the CA in-process (TlsConfig::with_root_certificates_pem); nothing is installed
# into a system store.
set -e
cd "$(dirname "$0")"

if [ "$1" = "" ] || [ "$2" = "" ]; then
    echo "Usage: keygen.sh [DOMAIN] [IP]"
    exit 1
fi

DOMAIN="$1"
IP="$2"
CA_NAME=${CA_NAME:-"rust-socketio-dev"}

mkdir -p cert
cd cert

if [ ! -f ca.key ]; then
    openssl genrsa -out ca.key 4096
fi

if [ ! -f ca.crt ]; then
    cat > ca.conf <<CONF
[ req ]
prompt = no
distinguished_name = dn
x509_extensions = ca_ext
[ dn ]
CN = ${CA_NAME}
[ ca_ext ]
basicConstraints = critical, CA:TRUE
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
CONF
    openssl req -x509 -new -nodes -key ca.key -days 3650 -config ca.conf -out ca.crt
fi

if [ ! -f server.key ]; then
    openssl genrsa -out server.key 2048
fi

if [ ! -f csr.conf ]; then
    cat > csr.conf <<CONF
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
CN = ${DOMAIN}

[ req_ext ]
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = ${DOMAIN}
DNS.2 = localhost
IP.1 = ${IP}
CONF
fi

if [ ! -f server.csr ]; then
    openssl req -new -key server.key -out server.csr -config csr.conf
fi

if [ ! -f server.crt ]; then
    # 825 days: the longest validity macOS accepts for a TLS server certificate
    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 825 \
        -out server.crt -extfile csr.conf -extensions req_ext
fi
