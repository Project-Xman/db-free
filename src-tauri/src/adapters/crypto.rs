// SOT: credential-encryption, aes-gcm, secret-at-rest, master-key

use crate::error::{AppError, AppResult};
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{Aes256Gcm, Nonce};

pub const KEY_LEN: usize = 32;
pub const NONCE_LEN: usize = 12;

// WHAT:  256-bit master key. Lives in the OS keychain, never on disk in the clear.
// WHY:   Connection passwords are sealed with it; losing it means re-entering
//        passwords, which is the correct failure mode.
// WHERE: src-tauri/src/adapters/keyring.rs (where it is stored)
#[derive(Clone)]
pub struct MasterKey([u8; KEY_LEN]);

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(<redacted>)")
    }
}

impl MasterKey {
    pub fn generate() -> MasterKey {
        let mut bytes = [0u8; KEY_LEN];
        OsRng.fill_bytes(&mut bytes);
        MasterKey(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> AppResult<MasterKey> {
        let arr: [u8; KEY_LEN] = bytes
            .try_into()
            .map_err(|_| AppError::crypto("master key has the wrong length"))?;
        Ok(MasterKey(arr))
    }

    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }
}

// WHAT:  AES-256-GCM seal: output is `nonce || ciphertext+tag`.
// HOW:   Fresh random nonce per call; GCM tag authenticates the whole blob.
pub fn seal(key: &MasterKey, plaintext: &[u8]) -> AppResult<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes()).map_err(AppError::crypto)?;
    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| AppError::crypto("encryption failed"))?;
    let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
    out.extend_from_slice(&nonce_bytes);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

pub fn open(key: &MasterKey, blob: &[u8]) -> AppResult<Vec<u8>> {
    if blob.len() < NONCE_LEN {
        return Err(AppError::crypto("sealed secret is too short"));
    }
    let (nonce_bytes, ciphertext) = blob.split_at(NONCE_LEN);
    let cipher = Aes256Gcm::new_from_slice(key.as_bytes()).map_err(AppError::crypto)?;
    let nonce = Nonce::from_slice(nonce_bytes);
    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| AppError::crypto("stored secret could not be decrypted (wrong master key?)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_then_open_round_trips() {
        let key = MasterKey::generate();
        let blob = seal(&key, b"hunter2").unwrap_or_default();
        assert_ne!(&blob[NONCE_LEN..], b"hunter2");
        assert_eq!(open(&key, &blob).unwrap_or_default(), b"hunter2");
    }

    #[test]
    fn wrong_key_fails_closed() {
        let blob = seal(&MasterKey::generate(), b"secret").unwrap_or_default();
        assert!(matches!(open(&MasterKey::generate(), &blob), Err(AppError::Crypto { .. })));
    }

    #[test]
    fn tampered_blob_fails() {
        let key = MasterKey::generate();
        let mut blob = seal(&key, b"secret").unwrap_or_default();
        if let Some(last) = blob.last_mut() {
            *last ^= 0xff;
        }
        assert!(open(&key, &blob).is_err());
    }
}
