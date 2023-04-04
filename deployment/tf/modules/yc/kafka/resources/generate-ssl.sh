#!/bin/bash

set -e

if [[ $# -lt 3 ]]; then
    echo "Usage: $0 <truststore-file> <keystore-file> <jks-password>"
    exit 1
fi

KEYSTORE_FILENAME=$2
VALIDITY_IN_DAYS=100
TRUSTSTORE_FILENAME=$1
CA_CERT_FILE="/tmp/ca-cert"
KEYSTORE_SIGN_REQUEST="/tmp/cert-file"
KEYSTORE_SIGNED_CERT="/tmp/cert-signed"

JKS_PASSWORD=$3

trust_store_private_key_file="/tmp/ca-key"

# Generate selfsigned CA and private key
openssl req -new -x509 -nodes \
  -keyout /tmp/ca-key \
  -out $CA_CERT_FILE \
  -days 100 \
  -subj /CN=kafka.lzy.ai/OU=DEV/O=LZY/L=./C=AU

# Load CA in jks truststore
keytool -keystore "$TRUSTSTORE_FILENAME" \
  -alias CARoot -import -file $CA_CERT_FILE -noprompt -storepass $JKS_PASSWORD

# Generate keystore
keytool -keystore $KEYSTORE_FILENAME -noprompt -storepass $JKS_PASSWORD \
  -dname "CN=kafka.lzy.ai, OU=DEV, O=LZY, L=.,C=AU" \
  -alias localhost -validity $VALIDITY_IN_DAYS -genkey -keyalg RSA

# Now a certificate signing request will be made to the keystore
keytool -keystore $KEYSTORE_FILENAME -alias localhost \
  -certreq -file $KEYSTORE_SIGN_REQUEST -noprompt -storepass $JKS_PASSWORD

# Now the trust store's private key (CA) will sign the keystore's certificate
openssl x509 -req -CA $CA_CERT_FILE -CAkey $trust_store_private_key_file \
  -in $KEYSTORE_SIGN_REQUEST -out $KEYSTORE_SIGNED_CERT \
  -days $VALIDITY_IN_DAYS -CAcreateserial
# creates $KEYSTORE_SIGN_REQUEST_SRL which is never used or needed.

keytool -keystore $KEYSTORE_FILENAME -alias CARoot -noprompt -storepass $JKS_PASSWORD \
  -import -file $CA_CERT_FILE

rm $CA_CERT_FILE # delete the trust store cert because it's stored in the trust store.

keytool -keystore $KEYSTORE_FILENAME -alias localhost -import -noprompt -storepass $JKS_PASSWORD \
  -file $KEYSTORE_SIGNED_CERT

rm $trust_store_private_key_file $KEYSTORE_SIGN_REQUEST $KEYSTORE_SIGN_REQUEST_SRL
