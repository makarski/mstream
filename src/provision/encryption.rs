use std::{
    env,
    path::{Path, PathBuf},
};

use aes_gcm::{
    AeadCore, Aes256Gcm, KeyInit,
    aead::{Aead, OsRng},
};
use anyhow::{Context, anyhow};
use tokio::fs;
use tracing::warn;

const DEFAULT_ENC_KEY_PATH: &str = "./mstream.key";
const ENC_KEY_VAR_NAME: &str = "MSTREAM_ENC_KEY";

pub struct Encryptor {
    cipher: Aes256Gcm,
}

pub struct EncryptedData {
    pub data: Vec<u8>,
    pub nonce: Vec<u8>,
}

impl Encryptor {
    pub fn new(key: &[u8]) -> anyhow::Result<Self> {
        let cipher = Aes256Gcm::new_from_slice(key)
            .context("failed to create AES-256-GCM cipher from key")?;
        Ok(Self { cipher })
    }

    pub fn encrypt(&self, b: &[u8]) -> anyhow::Result<EncryptedData> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, b)
            .map_err(|err| anyhow!("encryption failed: {}", err))?;

        Ok(EncryptedData {
            data: ciphertext,
            nonce: nonce.to_vec(),
        })
    }

    pub fn decrypt(&self, nonce: &[u8], b: &[u8]) -> anyhow::Result<Vec<u8>> {
        let nonce = aes_gcm::Nonce::from_slice(nonce);
        let plaintext = self
            .cipher
            .decrypt(nonce, b)
            .map_err(|err| anyhow!("decryption failed: {}", err))?;

        Ok(plaintext)
    }
}

pub(crate) async fn get_encryption_key(key_path: Option<&String>) -> anyhow::Result<Vec<u8>> {
    if let Ok(enc_key) = env::var(ENC_KEY_VAR_NAME) {
        let pbuf = PathBuf::from(format!("env:{}", ENC_KEY_VAR_NAME));
        return key_hex_decode(&enc_key, pbuf.as_path());
    }

    let path_buf = determine_key_path(key_path);
    let fs_path = path_buf.as_path();
    if fs_path.exists() {
        return encryption_key_from_path(fs_path).await;
    }

    warn!(
        "encryption key file not found at: {}. generating a new one.",
        fs_path.display()
    );

    generate_encryption_key_file(fs_path).await?;
    ensure_encryption_key_permissions(fs_path)?;
    encryption_key_from_path(fs_path).await
}

fn determine_key_path(key_path: Option<&String>) -> PathBuf {
    match key_path {
        Some(path) => Path::new(path).to_path_buf(),
        None => Path::new(DEFAULT_ENC_KEY_PATH).to_path_buf(),
    }
}

async fn encryption_key_from_path(fs_path: &Path) -> anyhow::Result<Vec<u8>> {
    let key_str = fs::read_to_string(fs_path).await.with_context(|| {
        format!(
            "failed to read encryption key from file: {}",
            fs_path.display()
        )
    })?;

    key_hex_decode(&key_str, fs_path)
}

fn key_hex_decode(key: &str, fs_path: &Path) -> anyhow::Result<Vec<u8>> {
    let key = key.trim();
    let key_bytes = hex::decode(&key).with_context(|| {
        format!(
            "failed to decode encryption key from hex: {}",
            fs_path.display()
        )
    })?;

    if key_bytes.len() != 32 {
        anyhow::bail!("invalid encryption key length: expected 32 bytes for AES-256");
    }

    Ok(key_bytes)
}

async fn generate_encryption_key_file(fs_path: &Path) -> anyhow::Result<()> {
    let encryption_key = Aes256Gcm::generate_key(OsRng);
    let key_hex = hex::encode(encryption_key.as_slice());

    fs::write(fs_path, key_hex)
        .await
        .context("failed to write encryption key to file")?;

    warn!(
        "generated new encryption key file at: {}. please, back it up securely, as it is required to access encrypted data.",
        fs_path.display()
    );

    Ok(())
}

fn ensure_encryption_key_permissions(fs_path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let metadata =
            fs::metadata(fs_path).context("failed to get encryption key file metadata")?;

        let mut permissions = metadata.permissions();
        permissions.set_mode(0o600); // Owner read/write only

        fs::set_permissions(fs_path, permissions)
            .context("failed to set encryption key file permissions")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes_gcm::KeyInit;
    use serial_test::serial;
    use tempfile::tempdir;
    use tokio::fs;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = Aes256Gcm::generate_key(OsRng);
        let encryptor = Encryptor::new(key.as_slice()).expect("cipher");
        let plaintext = b"super secret payload";

        let encrypted = encryptor.encrypt(plaintext).expect("encrypt");
        assert_ne!(encrypted.data, plaintext);

        let decrypted = encryptor
            .decrypt(&encrypted.nonce, &encrypted.data)
            .expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_with_wrong_key_fails() {
        let key_a = Aes256Gcm::generate_key(OsRng);
        let encryptor_a = Encryptor::new(key_a.as_slice()).unwrap();

        let key_b = Aes256Gcm::generate_key(OsRng);
        let encryptor_b = Encryptor::new(key_b.as_slice()).unwrap();

        let encrypted = encryptor_a.encrypt(b"data").unwrap();
        assert!(
            encryptor_b
                .decrypt(&encrypted.nonce, &encrypted.data)
                .is_err()
        );
    }

    #[tokio::test]
    #[serial]
    async fn key_loaded_from_env_var() {
        let key = hex::encode(Aes256Gcm::generate_key(OsRng));
        unsafe {
            env::set_var(ENC_KEY_VAR_NAME, &key);
        }

        let loaded = get_encryption_key(None).await.expect("load key");
        assert_eq!(loaded, hex::decode(key).unwrap());

        unsafe {
            env::remove_var(ENC_KEY_VAR_NAME);
        }
    }

    #[tokio::test]
    #[serial]
    async fn generates_new_key_file_when_missing() {
        unsafe {
            env::remove_var(ENC_KEY_VAR_NAME);
        }
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("enc.key");
        let file_str = file_path.to_string_lossy().to_string();

        assert!(!file_path.exists());
        let key = get_encryption_key(Some(&file_str)).await.expect("generate");
        assert_eq!(key.len(), 32);
        assert!(file_path.exists());

        let stored = fs::read_to_string(&file_path).await.unwrap();
        assert_eq!(hex::decode(stored.trim()).unwrap(), key);
    }

    #[tokio::test]
    #[serial]
    async fn loads_existing_key_file() {
        unsafe {
            env::remove_var(ENC_KEY_VAR_NAME);
        }
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("existing.key");
        let key = Aes256Gcm::generate_key(OsRng);
        fs::write(&file_path, hex::encode(key.as_slice()))
            .await
            .unwrap();

        let key_str = file_path.to_string_lossy().to_string();
        let loaded = get_encryption_key(Some(&key_str)).await.unwrap();
        assert_eq!(loaded, key.as_slice());
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn generated_key_file_has_strict_permissions() {
        use std::os::unix::fs::PermissionsExt;

        unsafe {
            env::remove_var(ENC_KEY_VAR_NAME);
        }
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("perm.key");
        let key_str = file_path.to_string_lossy().to_string();

        get_encryption_key(Some(&key_str)).await.unwrap();

        let metadata = std::fs::metadata(&file_path).unwrap();
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn determine_key_path_uses_default_when_none() {
        let path = determine_key_path(None);
        assert_eq!(path, PathBuf::from(DEFAULT_ENC_KEY_PATH));
    }

    #[test]
    fn determine_key_path_uses_provided_path() {
        let provided = "custom.key".to_string();
        let path = determine_key_path(Some(&provided));
        assert_eq!(path, PathBuf::from("custom.key"));
    }

    #[tokio::test]
    #[serial]
    async fn fails_on_invalid_key_length() {
        unsafe {
            env::remove_var(ENC_KEY_VAR_NAME);
        }
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("short.key");

        // Write a key that is too short (e.g., 16 bytes instead of 32)
        fs::write(&file_path, hex::encode([0u8; 16])).await.unwrap();

        let key_str = file_path.to_string_lossy().to_string();
        let result = get_encryption_key(Some(&key_str)).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid encryption key length")
        );
    }
}
