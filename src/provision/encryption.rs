use std::{
    env,
    path::{Path, PathBuf},
};

use aes_gcm::{
    AeadCore, Aes256Gcm, KeyInit,
    aead::{AeadMut, OsRng},
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

    pub fn encrypt(&mut self, b: &[u8]) -> anyhow::Result<EncryptedData> {
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
        let mut cipher = self.cipher.clone();
        let plaintext = cipher
            .decrypt(nonce, b)
            .map_err(|err| anyhow!("decryption failed: {}", err))?;

        Ok(plaintext)
    }
}

pub(crate) async fn get_encryption_key(key_path: Option<&String>) -> anyhow::Result<Vec<u8>> {
    if let Ok(enc_key) = env::var(ENC_KEY_VAR_NAME) {
        return hex::decode(&enc_key).context("failed to decode encryption key from hex");
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
    let key_hex = fs::read_to_string(fs_path)
        .await
        .context("failed to read encryption key from file")?;

    let key_bytes = hex::decode(&key_hex).context("failed to decode encryption key from hex")?;

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
