use std::ffi::OsStr;
use std::path::{Component, Path};

use anyhow::{Context, anyhow};
use tokio::fs;

use crate::config::service_config::{UdfConfig, UdfScript};

pub fn validate_script_filename(filename: &str) -> anyhow::Result<()> {
    if filename.is_empty() {
        anyhow::bail!("invalid script filename: empty");
    }

    let path = Path::new(filename);
    let mut components = path.components();

    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => {}
        _ => anyhow::bail!("invalid script filename '{}'", filename),
    }

    if path.extension() != Some(OsStr::new("rhai")) {
        anyhow::bail!(
            "invalid script filename '{}': must have .rhai extension",
            filename
        );
    }

    Ok(())
}

pub async fn create_udf_script(cfg: &UdfConfig) -> anyhow::Result<()> {
    let base_path = Path::new(&cfg.script_path);

    let sources = match &cfg.sources {
        Some(s) => s,
        None => &vec![],
    };

    if sources.is_empty()
        && (!base_path.exists() || fs::read_dir(base_path).await?.next_entry().await?.is_none())
    {
        anyhow::bail!(
            "no udf sources provided and udf script dir is empty or does not exist for service: {}. script_path: {}",
            cfg.name,
            base_path.display()
        );
    }

    if !base_path.exists() {
        fs::create_dir_all(base_path).await.with_context(|| {
            anyhow!(
                "failed to create an udf script dir: {}",
                base_path.display()
            )
        })?;
    }

    for source in sources {
        validate_script_filename(&source.filename)?;

        let script_path = base_path.join(&source.filename);
        tokio::fs::write(&script_path, &source.content)
            .await
            .with_context(|| {
                anyhow!("failed to write udf script file: {}", script_path.display())
            })?;
    }

    Ok(())
}

pub async fn read_udf_scripts(script_path: &Path) -> anyhow::Result<Vec<UdfScript>> {
    let mut dir = fs::read_dir(&script_path).await.with_context(|| {
        anyhow!(
            "failed to read udf script directory: {}",
            script_path.display()
        )
    })?;

    let mut scripts = Vec::new();

    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.is_file() && path.extension() == Some(OsStr::new("rhai")) {
            let content = fs::read_to_string(&path)
                .await
                .with_context(|| anyhow!("failed to read udf script file: {}", path.display()))?;

            let filename = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                anyhow!("failed to read filename from a script: {}", path.display())
            })?;

            scripts.push(UdfScript {
                filename: filename.to_string(),
                content,
            });
        }
    }

    Ok(scripts)
}
